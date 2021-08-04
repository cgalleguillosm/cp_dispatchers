import argparse
from builtins import str, int

from accasim.base.resource_manager_class import Resources
from accasim.base.simulator_class import Simulator
from accasim.utils.misc import obj_assertion
from accasim.utils.reader_class import DefaultTweaker

from pcp21_dispatcher import PCP21
from predictor import SWFLastNPredictorInterface


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


class KIT_Tweaker(DefaultTweaker):

    def __init__(self, start_time, system_resources=None, equivalence=None):
        """

        :param start_time:
        :param equivalence:
        """
        if system_resources:
            obj_assertion(system_resources, Resources)
        self.start_time = start_time
        self.equivalence = equivalence if equivalence else {'processor': {'core': 1}}

    def gcdWMAX(self, a, b, max):
        """Calculate the Greatest Common Divisor of a and b.
    
        Unless b==0, the result will have the same sign as b (so that when
        b is divided by it, the result comes out positive).
        """
        while b or a > max:
            a, b = b, a % b
        return a

    def tweak_function(self, _dict):
        total_core = _dict.pop('total_processors') if _dict['total_processors'] != -1 else _dict.pop(
            'allocated_processors')
        _dict['queue'] = _dict['partition_number']
        thin_node = (_dict['queue'] == 1)

        cores_per_node = 20 if thin_node else 48

        max_cores_per_node = (self.gcdWMAX(total_core, cores_per_node,
                                           cores_per_node) if total_core % cores_per_node > 0 else cores_per_node) if total_core >= cores_per_node else total_core
        requested_nodes = min(total_core // max_cores_per_node if total_core >= cores_per_node else 1,
                              1152 if thin_node else 21)
        requested_resources = {'core': max_cores_per_node}

        if thin_node:
            assert (1 <= requested_nodes <= 1152), 'More nodes requested than the available: {}'.format(_dict)
            assert (1 <= max_cores_per_node <= 20), 'More cores requested than the available: {}'.format(_dict)
        else:
            assert (1 <= requested_nodes <= 21), 'More nodes requested than the available: {}'.format(_dict)
            assert (1 <= max_cores_per_node <= 48), 'More cores requested than the available: {}'.format(_dict)

        _dict['queue'] = _dict['queue_number']
        _dict['core'] = requested_nodes * max_cores_per_node
        _dict['queued_time'] = _dict.pop('queued_time') + self.start_time
        _dict['requested_nodes'] = requested_nodes
        _dict['requested_resources'] = requested_resources

        return _dict


def simulate(workload, sys_config, results_folder='results', **kwargs):
    """

    :param workload: Path to the workload file
    :param sys_config: Path to the config file
    :param results_folder: Path where output files will be saved
    :param kwargs:
    :return:
    """
    adata = []
    otherkws = {}

    jf = None
    reader = None

    parser = kwargs.pop('parser')
    if parser == 'KIT':
        tweaker = KIT_Tweaker(1464739965)
        otherkws['tweak_function'] = tweaker
    else:
        raise Exception('Parser not implemented')

    dispatcher = PCP21(**kwargs)

    otherkws['additional_data'] = adata
    if kwargs.get('prediction', False):
        if parser == 'KIT':
            adata.append(SWFLastNPredictorInterface)
        otherkws['EXTENDED_JOB_DESCRIPTION'] = True

    simulator = Simulator(workload, sys_config, dispatcher, job_factory=jf, reader=reader,
                          RESULTS_FOLDER_NAME=results_folder, scheduling_output=kwargs.get('scheduling_output', True),
                          statistics_output=kwargs.get('statistics_output', True),
                          benchmark_output=kwargs.get('benchmark_output', True),
                          pprint_output=kwargs.get('pprint_output', False), LOG_LEVEL=Simulator.LOG_LEVEL_INFO,
                          **otherkws)
    simulator.start_simulation()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('workload', metavar='W', type=str, help='Workload filepath')
    parser.add_argument('sys_config', metavar='S', type=str, help='Sys config filepath')
    parser.add_argument('--simulator_config', type=str, help='Essentials filepath')
    parser.add_argument('--parser', type=str, help='KIT, ...', default='KIT')
    parser.add_argument('--results_folder', type=str, help='Path for the output files')
    parser.add_argument('--safe', type=str2bool,
                        help='Run in a child thread so if the solver crashes the simulation continues', default=False)
    parser.add_argument('--reduced_model', type=str2bool,
                        help='True will consider only jobs which can be scheduled at current time', default=True)
    parser.add_argument('--max_retries', type=int,
                        help='Each retry increase 1 sec for searching a solution. Starts with 1 sec.', default=30)
    parser.add_argument('--sched_bt', type=str2bool, help='Backtracks only to scheduling variables', default=True)
    parser.add_argument('--prediction', type=str2bool, help='Enable the job duration prediction', default=False)
    parser.add_argument('--initial_timelimit', type=int, help='Initial timelimit', default=1000)
    parser.add_argument('--q_length', type=int, help='Initial Q Length', default=100)
    parser.add_argument('--element_v2', type=str2bool, help='Element constraint proposed in reviews', default=False)
    parser.add_argument('--sim_break_alloc', type=str2bool, help='Symmetry Breaking Allocation', default=True)

    args = parser.parse_args()
    mandatory = ['workload', 'sys_config']
    print('Executing {} on {}'.format(args.workload, args.sys_config))
    simulate(*(getattr(args, m) for m in mandatory),
             **{k: v for k, v in args.__dict__.items() if not (k in mandatory) and v != None})
