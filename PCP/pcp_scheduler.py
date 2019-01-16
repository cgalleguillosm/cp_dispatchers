"""
MIT License

Copyright (c) 2019 cgalleguillosm

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from sortedcontainers.sortedlist import SortedListWithKey, SortedList
import time
from collections import namedtuple
from itertools import groupby

from ortools.constraint_solver import pywrapcp
from accasim.base.scheduler_class import SchedulerBase, DispatcherError
from enum import Enum

class PCP(SchedulerBase):
    """
    A scheduler which uses constraint programming to plan a sub-optimal schedule.
    This scheduler don't use the automatic feature of the automatic allocation, then it isn't

    """
    name = 'PCP'
    
    class SolverState(Enum):
        OUTSIDE_SEARCH = None  # Before search, after search.
        IN_ROOT_NODE = 1  # Executing the root node.
        IN_SEARCH = 2  # Executing the search code.
        AT_SOLUTION = 3  # After successful NextSolution and before EndSearch.
        NO_MORE_SOLUTIONS = 4  # After failed NextSolution and before EndSearch.
        PROBLEM_INFEASIBLE = 5  # After search, the model is infeasible.

    def __init__(self, seed=0, **kwargs):
        SchedulerBase.__init__(self, seed, None)

        # kwargs 
        self.ewt = kwargs.pop('ewt', {'default': 1600})
        self.debug = True if kwargs.pop('debug', 'INFO') != 'INFO' else False
        self.reduce_job_length = kwargs.pop('reduce_job_length', True)
        self.timelimit = kwargs.pop('timelimit', 125 * 1000)
        self.initial_timelimit = kwargs.pop('initial_timelimit', 60 * 1000)
        self.significant = kwargs.pop('significant', 5)                      
    
    def scheduling_method(self, cur_time, es, es_dict):
        """
            This function must map the queued events to available nodes at the current time.

            :param cur_time: current time
            :param es_dict: dictionary with full data of the events
            :param es: events to be scheduled
            :param debug: Flag to debug

            :return a tuple of (time to schedule, event id, list of assigned nodes)
        """
        resource_types = self.resource_manager.resource_types
        avl_resources = self.resource_manager.current_availability
        
        cons_qjobs = [e.id for e in es]
        wc_makespan = None
        makespans = None
        remaining_priorized_jobs = None
        decision_jobs = {}

        timelimit = self.initial_timelimit
                
        solved = False
                
        while not solved and timelimit <= self.timelimit:

            schedalloc_plan = {}
            args = (schedalloc_plan, cur_time, cons_qjobs, es_dict, wc_makespan, makespans, resource_types, avl_resources)
            kwargs = {'timelimit':timelimit}
            solved = self.cp_model(*args, **kwargs)

            disp_jobs = sum([(1 if stime else 0) for stime, _, _ in schedalloc_plan.values()])

            timelimit *= 2
        
        return list(schedalloc_plan.values()) + list(decision_jobs.values()), []
    
    def allocate(self, cur_time, schedule_plan, es_dict, debug):
        """
        Prepare jobs to be allocated. Just jobs that must be started at the current time are considered.
        @param cur_time: Current simulation time
        @param schedule_plan: schedule plan
        @param es_dict: Dictionary of the current jobs.
        @param debug: Debug flag

        @return: dispatching plan
        """
        allocated_events = []
        to_schedule_now = []
        to_schedule_later = []
        # Building the list of jobs that can be allocated now, in the computed solution
        for _id, (_time, _, _) in schedule_plan.items():
            if _time == cur_time:
                to_schedule_now.append(es_dict[_id])
            else:
                to_schedule_later.append(es_dict[_id])
        # Trying to allocate all jobs scheduled now
        allocation = self.manual_allocator.allocate(to_schedule_now, cur_time, skip=True)  # , debug=debug)
        # All jobs to be scheduled later are considered as discarded by the allocator
        allocated_events += [self.dispatching_tuple(ev.id) for ev in to_schedule_later]
        return allocation + [self.dispatching_tuple(ev.id) for ev in to_schedule_later]

    def cp_model(self, decision_jobs, cur_time, cons_qjobs, es_dict, wc_makespan, makespans, resource_types, available_resource, solution_rate=1, timelimit=15000, debug=False, jobs_priority=None):
        # Current system capacity
        nodes_avl = self.resource_manager.system_capacity('nodes')
        
        # Dictionary with the current running jobs
        # Key is the id of the job_i
        # Value is the job_i object
        running_jobs = [es_dict[e] for e in self.resource_manager.current_allocations]
        # List of queued jobs (objects) 
        queued_jobs = [es_dict[e] for e in cons_qjobs]
        
        # Running + Queued job_i objects
        current_jobs = running_jobs + queued_jobs
        
        wc_makespan = sum([ job.expected_duration - (cur_time - job.start_time) if job.start_time else job.expected_duration for job in current_jobs])
            
        solver = pywrapcp.Solver('PCP')
        
        tasks = {}
        for job in current_jobs:
            job_id = job.id
            exp_duration = job.expected_duration
            max_start = wc_makespan
            if job.start_time:
                max_start = 0
                exp_duration -= (cur_time - job.start_time)
            else:
                exp_duration = max(1, job.expected_duration)
            interval = solver.FixedDurationIntervalVar(0, max_start, exp_duration, False, job_id)
            tasks[job_id] = interval

        utils_nodes = {}
        all_intervals = []
        for job in current_jobs:
            job_id = job.id
            exp_duration = job.expected_duration
            max_start = wc_makespan
            assigned_nodes = {}
            if job.start_time:
                max_start = 0
                exp_duration -= (cur_time - job.start_time)
                assigned_nodes = {k: len(list(v)) for k, v in groupby(job.assigned_nodes)}
            else:
                exp_duration = max(1, job.expected_duration)

            utils_nodes[job.id] = {}
            for node, node_avl in nodes_avl.items():
                utils_nodes[job.id][node] = []
                for k in range(self._joint_nodes(job, node_avl)):
                    name = 'util_{}_{}_{}'.format(job_id, node, k)
                    interval = None
                    if job.start_time:
                        if node in assigned_nodes and k < assigned_nodes[node]:
                            interval = solver.FixedDurationIntervalVar(0, max_start, exp_duration, False, name)
                    else:
                        interval = solver.FixedDurationIntervalVar(0, max_start, exp_duration, True, name)
                        
                    if interval:
                        utils_nodes[job.id][node].append(interval)
                        all_intervals.append(interval)
        
        for job in current_jobs:
            job_id = job.id
            alternatives = []
            for node, node_alts in utils_nodes[job_id].items():
                alternatives.extend(node_alts)
            alternative_cst = solver.Alternative(tasks[job_id], alternatives, job.requested_nodes)
            solver.Add(alternative_cst)
        
        for res_type in resource_types:
            for node, node_avl in nodes_avl.items():
                intervals = []
                demands = []
                capacity = node_avl[res_type]
                cum_name = 'cum_cst_{}_{}'.format(res_type, node)
                for job in current_jobs:
                    job_id = job.id
                    if node in utils_nodes[job_id]:
                        alts = utils_nodes[job_id][node]
                        intervals.extend(alts)
                        demands.extend([job.requested_resources[res_type] for _ in range(len(alts))])  
                cumulative_cst = solver.Cumulative(intervals, demands, capacity, cum_name)
                solver.Add(cumulative_cst)
        #=======================================================================
        # Objective function
        #=======================================================================
        ewts = []      
        digits = max(self.significant, len(str(wc_makespan)))
        
        for job_i in current_jobs:
            _id = str(job_i.id)
            ewt = self.get_ewt(job_i.queue)
            cur_waiting_time = (cur_time - job_i.queued_time)
            job_start = tasks[_id].StartExpr()
            #
            ewt_job = solver.Max(((job_start + cur_waiting_time - ewt) * 10 ** digits) // ewt, solver.IntConst(0))
            ewts.append(ewt_job)

        # Minimize the weighted queue time
        objective_var = solver.Sum(ewts).Var()
        objective_monitor = solver.Minimize(objective_var, 1)

        db = solver.Phase(all_intervals, solver.INTERVAL_DEFAULT)
        limit = solver.TimeLimit(timelimit)

        solver.NewSearch(db, objective_monitor, limit)
        
        solved = False
        running_ids = [job.id for job in running_jobs]

        disp_jobs = 0
        
        while solver.NextSolution():
            solved = True                
            disp_jobs = 0
            for job_id, job_interval in tasks.items():
                if job_id in running_ids:
                    continue
                if job_interval.MustBePerformed() and job_interval.StartMin() == 0:
                    nodes = []
                    for node_id, job_units in utils_nodes[job_id].items():
                        for node in job_units:
                            if node.MustBePerformed():
                                nodes.append(node_id)
                    if nodes:
                        decision_jobs[job_id] = self.dispatching_tuple(job_id, cur_time, nodes)
                        disp_jobs += 1
                    else:
                        decision_jobs[job_id] = self.dispatching_tuple(job_id)
                else:
                    decision_jobs[job_id] = self.dispatching_tuple(job_id)
        
        if disp_jobs == 0:
            solved = False
            
        solver.EndSearch()            
        solver = None        
        if not solved:
            for _e in cons_qjobs:
                decision_jobs[_e] = self.dispatching_tuple(_e)    
                
        return solved
    
    def get_id(self):
        """
        Returns the full ID of the dispatcher.

        :return: the dispatcher's id.
        """ 
        return self.name

    def fitting_job_unit(self, node_resources, job, debug=False):
        req_nodes = job.requested_nodes
        req_resources = job.requested_resources
        for res, avl_res in node_resources.items():
            if avl_res and req_resources[res]:
                req_nodes = min(req_nodes, (avl_res // (req_resources[res])))
        return req_nodes   

    def get_ewt(self, queue_type):
        """
        @param queue_type: Name of the queue.

        @return: EWT for a specific queue type
        """
        if queue_type in self.ewt:
            return self.ewt[queue_type]
        return self.ewt['default']
        
    def dispatching_tuple(self, job_id, time=None, allocation=[]):
        assert(isinstance(job_id, str)), 'For job_id only str is accepted'
        return (time, job_id, allocation)
        
    def _joint_nodes(self, job, node_avl, res_type=None):
        """
        Calculate how many times an specific node can hold the job request
        :param job: Job to be fitted
        :param node_avl: Node under fitting
        :param res_type: Type of resource. If res_type is None, all available resources are considered
        :return: An integer number with the fitting times.
        """
        if not res_type:
            _min = min([node_avl[res_type] // v for res_type, v in job.requested_resources.items() if v > 0])
            return _min
        return node_avl[res_type] // job.requested_resources[res_type]
