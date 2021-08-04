import sys
from abc import abstractmethod
from itertools import chain
from time import time

from accasim.base.scheduler_class import SchedulerBase
from accasim.utils.misc import CONSTANT
from ortools.constraint_solver import pywrapcp
from pathos.helpers import mp as mp_dill
from sortedcontainers.sortedlist import SortedListWithKey, SortedList


class Dispatcher(SchedulerBase):

    def __init__(self, resource_manager=None, seed=0, **kwargs):
        SchedulerBase.__init__(self, seed, None)
        self.constants = CONSTANT()
        self._safe = kwargs.pop('safe', False)
        self._cur_q_length = self._q_length = kwargs.pop('q_length', 100)
        self._reduce_job_length = kwargs.pop('reduce_job_length', True)
        self._initial_timelimit = kwargs.pop('initial_timelimit', 1000)
        self._max_timelimit = kwargs.pop('timelimit', 16000)
        self._reduced_model = kwargs.pop('reduced_model', True)
        self._considered_cannot_start = kwargs.pop('considered', 1)
        self._max_retries = kwargs.pop('max_retries', 4)
        self._trace_search = kwargs.pop('trace_search', False)
        self._trace_propagation = kwargs.pop('trace_propagation', False)
        self._sched_bt = kwargs.pop('sched_bt', True)
        self._element_v2 = kwargs.pop('element_v2', False)
        self._sim_break_alloc = kwargs.pop('sim_break_alloc', True)
        self._defined_resource_types = None
        self._resources_map = self._nodes_map = self._c_capacities = None

    def scheduling_method(self, cur_time, es, es_dict):
        """
            This function must map the queued events to available nodes at the current time.

            :param cur_time: current time
            :param es_dict: dictionary with full data of the events
            :param es: events to be scheduled
            :param debug: Flag to debug

            :return a tuple of (time to schedule, event id, list of assigned nodes)
        """
        dispatching_plan = []

        resource_types = self.resource_manager.resource_types
        avl_resources = self.resource_manager.current_availability
        system_capacity = self.resource_manager.system_capacity('nodes')

        # =======================================================================
        # Considered queued jobs: Jobs can be fitted in the current system state and less or equal than q_length
        # If a job_obj cannot be fitted or exceed the q_length is directly loaded in the dispatching decision using the no-solution dispatching tuple
        # =======================================================================
        priorized_jobs = SortedListWithKey(key=lambda job_tuple: job_tuple[1])

        current_qjobs = SortedList()

        cons_qjobs = {}
        for node in self.resource_manager.node_names:
            avl_res = avl_resources[node]
            # avl_res = system_capacity[node]
            for idx, job_obj in enumerate(es):
                job_id = job_obj.id

                if not (job_id in cons_qjobs):
                    current_qjobs.add(job_id)
                    cons_qjobs[job_id] = [False, 0, {}, None]
                    priorized_jobs.add((job_id, self._job_priority_slowdown(job_obj, cur_time)))
                if self._reduced_model:
                    possibilities = self._joint_nodes(job_obj, avl_res)
                    if possibilities > 0:
                        cons_qjobs[job_id][2][node] = min(possibilities, job_obj.requested_nodes)
                        cons_qjobs[job_id][1] += possibilities
                        if cons_qjobs[job_id][1] >= job_obj.requested_nodes:
                            cons_qjobs[job_id][0] = True
                            if not cons_qjobs[job_id][3]:
                                cons_qjobs[job_id][3] = job_obj
                else:
                    cons_qjobs[job_id][0] = True
                    cons_qjobs[job_id][1] = None
                    cons_qjobs[job_id][2] = None
                    cons_qjobs[job_id][3] = job_obj

        qjobs = 0
        wc_makespan = 0
        makespans = []

        selected_priorized_jobs = []

        # Job of the dispatching decision
        decision_jobs = {}

        if self._reduced_model:
            for job_id, _ in priorized_jobs:
                t = cons_qjobs[job_id]
                if not t[0] or qjobs > self._cur_q_length - 1:
                    decision_jobs[job_id] = self.dispatching_tuple(job_id)
                    cons_qjobs.pop(job_id)
                else:
                    exp_duration = max(1, t[-1].expected_duration)
                    wc_makespan += exp_duration
                    makespans.append(exp_duration)
                    qjobs += 1
                    selected_priorized_jobs.append(job_id)
        else:
            cannot_start_selected = 0
            for job_id, _ in priorized_jobs:
                t = cons_qjobs[job_id]
                if (not t[0] and cannot_start_selected >= self._considered_cannot_start) or (
                        qjobs > self._cur_q_length - 1):
                    decision_jobs[job_id] = self.dispatching_tuple(job_id)
                    cons_qjobs.pop(job_id)
                else:
                    if not t[0]:
                        cons_qjobs[job_id][3] = es_dict[job_id]
                        cannot_start_selected += 1
                    exp_duration = max(1, t[-1].expected_duration)
                    wc_makespan += exp_duration  # , self.get_queue(t[-1].queue))  # exp_duration
                    makespans.append(exp_duration)
                    qjobs += 1
                    selected_priorized_jobs.append(job_id)
        # =======================================================================
        # There are no jobs to dispatch at the current system state.
        # Then a no solution list is returned.
        # =======================================================================
        if not cons_qjobs:
            # Job Dispatching skip
            return decision_jobs.values(), []

        solved = False
        self.priorized_jobs = None

        if self._safe:
            manager = mp_dill.Manager()
            schedule_plan = manager.dict()
            process_class = mp_dill.Process

            p = process_class(target=getattr(self, 'cp_model'),
                              args=(
                                  schedule_plan, cur_time, cons_qjobs, selected_priorized_jobs, es_dict, resource_types,
                                  avl_resources),
                              kwargs={'timelimit': timelimit}
                              )
            p.start()
            p.join()

            if p.exitcode != 0:
                schedule_plan.pop('solver_state', None)
                schedule_plan.pop('limit_reached', None)
                return list(decision_jobs.values()) \
                       + [self.dispatching_tuple(job_id, start_time, nodes) for (start_time, job_id, nodes) in
                          schedule_plan.values()] \
                       + [self.dispatching_tuple(job_id, None, []) for job_id in cons_qjobs if
                          not (job_id in schedule_plan)], []
        else:
            schedule_plan = {}
            args = (
                schedule_plan, cur_time, cons_qjobs, selected_priorized_jobs, es_dict, resource_types, avl_resources)
            kwargs = {'max_timelimit': self._max_timelimit}
            function = getattr(self, 'cp_model')
            function(*args, **kwargs)

        solved = schedule_plan.pop('solved')
        of_value = schedule_plan.pop('of_value')
        walltime = schedule_plan.pop('walltime')
        proc_time = schedule_plan.pop('proc_time')
        incurred_time = walltime + proc_time
        failures = schedule_plan.pop('failures')
        branches = schedule_plan.pop('branches')
        p = None

        self.priorized_jobs = None
        dispatching_plan = list(schedule_plan.values())
        self.__instance_data = (
            solved, of_value, walltime, incurred_time, failures, branches,
            dispatching_plan + list(decision_jobs.values()),)

        # This is useful for print and also to create the unsuccessful data
        dispatched_jobs = 0
        queued_job_ids = []
        for a in dispatching_plan:
            if a[2]:
                dispatched_jobs += 1
            if dispatched_jobs == 0:
                queued_job_ids.append(a[1])

        if self._reduce_job_length:
            # ===================================================================
            # The considered number of jobs in the next scheduling decision are reduced to the half
            # if the current problem instance was not solved, if the current usage is
            # leq of the previous time point. After a successful dispatching this value is reset.
            # The minimum is 1, otherwise there will be nothing to dispatch
            # ===================================================================
            if not solved:
                self._cur_q_length = max(1, min(self._cur_q_length,
                                                len(schedule_plan)) // 2)  # max(1, self._cur_q_length // 2)
            else:
                self._cur_q_length = self._q_length

        print('{} - {}: Queued {}, Dispatched {}, Running {}. {}'.format(self._counter, cur_time,
                                                                         len(es) - dispatched_jobs, dispatched_jobs,
                                                                         len(self.resource_manager.current_allocations),
                                                                         self.resource_manager.current_usage))
        return dispatching_plan + list(decision_jobs.values()), []

    @abstractmethod
    def cp_model(self):
        pass

    def get_id(self):
        """
        Returns the full ID of the scheduler, including policy and allocator.

        :return: the scheduler's id.
        """
        return self.name

    def _job_priority_slowdown(self, job_obj, cur_time):
        exp_duration = job_obj.expected_duration
        wait_time = cur_time - job_obj.queued_time
        if exp_duration == 0:
            job_slowdown = max(1, wait_time)
        else:
            job_slowdown = (exp_duration + wait_time) / exp_duration
        return (-job_slowdown
                , job_obj.expected_duration,
                job_obj.expected_duration * sum(
                    [job_obj.requested_nodes * val for attr, val in job_obj.requested_resources.items()])
                )

    def dispatching_tuple(self, job_id, time=None, allocation=[]):
        return (time, job_id, allocation)


class PCP21(Dispatcher):
    name = 'PCP21'

    def map_resources(self, nodes_capacity, resource_types):
        if not self._resources_map:
            resources_map = {}
            nodes_map = {}
            c_capacities = {}
            for resource_type in resource_types:
                if not (resource_type in resources_map):
                    resources_map[resource_type] = []
                    nodes_map[resource_type] = {}
                    c_capacities[resource_type] = 0

            for node_pos, (node_id, node_capacities) in enumerate(nodes_capacity.items()):
                for resource_type, capacity in node_capacities.items():
                    for c in range(capacity):
                        resources_map[resource_type].append(node_pos + 1)
                    if capacity > 0:
                        nodes_map[resource_type][node_id] = (
                            c_capacities[resource_type] + 1, c_capacities[resource_type] + capacity)
                        c_capacities[resource_type] += capacity
            self._resources_map = resources_map
            self._nodes_map = nodes_map
            self._c_capacities = c_capacities
        return self._resources_map, self._nodes_map, self._c_capacities

    def defined_resource_types(self):
        # make dynamic -.-
        if not self._defined_resource_types:
            resource_types = []
            if 'gpu' in self.resource_manager.resource_types:
                resource_types = ['gpu'] + resource_types
            if 'mic' in self.resource_manager.resource_types:
                resource_types = ['mic'] + resource_types
            if 'mem' in self.resource_manager.resource_types:
                resource_types = ['mem'] + resource_types
            if 'thin' in self.resource_manager.resource_types:
                resource_types = ['thin'] + resource_types
            resource_types += ['core']
            self._defined_resource_types = resource_types
        return self._defined_resource_types

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

    def cp_model(self, dispatching_decision, cur_time, cons_qjobs, priorized_jobs, es_dict, resource_types,
                 avl_resources, max_timelimit=15000, debug=False):
        # resource_types = ['gpu', 'mic', 'core', 'mem']  if 'gpu' in self.resource_manager.resource_types else self.resource_manager.resource_types
        resource_types = self.defined_resource_types()
        avl_resources = self.resource_manager.current_availability  # Avl resources (with consumed resources)

        nodes_capacity = self.resource_manager.system_capacity('nodes')  # Avl resources (without consumed resources)
        resources_map, nodes_map, total_resource_capacities = self.map_resources(nodes_capacity,
                                                                                 self.resource_manager.resource_types)

        parameters = pywrapcp.Solver_DefaultSolverParameters()
        parameters.trace_search = self._trace_search
        parameters.trace_propagation = self._trace_propagation
        solver = pywrapcp.Solver('composed', parameters)

        # Makespan of queued jobs
        mks = sum([max(1, t[3].expected_duration) for t in cons_qjobs.values()])

        running_jobs = self.resource_manager.current_allocations

        job_durations = {}
        box_vars = {resource_type: {'ivs': [], 'x': [], 'dx': [], 'y': [], 'dy': []} for resource_type in
                    resource_types}
        job_intervals_dict = {}
        jobs_allocations = {}
        decision_sched_vars = []
        decision_allocation_vars = {}
        start_vars_array = []
        start_vars = {}

        current_node_usage_y = {}

        for job_id in running_jobs:
            job_obj = es_dict[job_id]
            expected_duration = max(1, job_obj.expected_duration)
            mks += expected_duration
            job_durations[job_id] = expected_duration

            x = solver.IntConst(0, 'x_{}'.format(job_id))
            start_vars[job_id] = x
            start_vars_array.append(x)
            interval = solver.FixedDurationIntervalVar(x, expected_duration, job_id)
            dx = solver.IntConst(expected_duration, 'DX_{}'.format(job_id))
            jobs_allocations[job_id] = []
            decision_allocation_vars[job_id] = []
            for node_id, consumed_resources in running_jobs[job_id].items():
                if not (node_id in current_node_usage_y):
                    current_node_usage_y[node_id] = {resource_type: 0 for resource_type in resource_types}
                for resource_type, consumed_resource in consumed_resources.items():
                    if not (resource_type in resource_types):
                        continue
                    if consumed_resource == 0:
                        continue
                    current_node_resource_usage = current_node_usage_y[node_id][resource_type]
                    y_used = (nodes_map[resource_type][node_id][0] - 1 + current_node_resource_usage,
                              nodes_map[resource_type][node_id][0] - 1 + current_node_resource_usage)

                    current_node_usage_y[node_id][resource_type] += consumed_resource

                    y = solver.IntVar(*y_used, 'Y_{}_{}_{}'.format(job_id, node_id, resource_type))
                    dy = solver.IntConst(consumed_resource, 'DY_{}_{}_{}'.format(job_id, node_id, resource_type))

                    box_vars[resource_type]['ivs'].append(interval)
                    box_vars[resource_type]['x'].append(x)
                    box_vars[resource_type]['dx'].append(dx)
                    box_vars[resource_type]['y'].append(y)
                    box_vars[resource_type]['dy'].append(dy)

                    if resource_type == 'core':
                        decision_allocation_vars[job_id].append(y)

            interval = solver.FixedDurationIntervalVar(x, expected_duration, job_id)
            decision_sched_vars.append(interval)
            job_intervals_dict[job_id] = interval

        x_dict = {}
        for job_id in priorized_jobs:
            job_obj = es_dict[job_id]
            expected_duration = max(1, job_obj.expected_duration)
            job_durations[job_id] = expected_duration

            x = solver.IntVar(0, mks, 'x_{}'.format(job_id))
            x_dict[job_id] = x
            start_vars[job_id] = x
            start_vars_array.append(x)
            dx = solver.IntConst(expected_duration, 'DX_{}'.format(job_id))

            interval = solver.FixedDurationIntervalVar(x, expected_duration, job_id)
            decision_sched_vars.append(interval)
            job_intervals_dict[job_id] = interval

            y_vars = []
            requested_resources = job_obj.requested_resources

            elements = {}

            decision_allocation_vars[job_id] = []

            for resource_type in resource_types:
                y_var = []
                if requested_resources[resource_type] == 0:
                    continue
                dy = solver.IntConst(requested_resources[resource_type], 'DY_{}_{}'.format(job_id, resource_type))
                max_initial_position = total_resource_capacities[resource_type] - requested_resources[resource_type]
                for i in range(job_obj.requested_nodes):
                    y = solver.IntVar(0, max_initial_position,
                                      'Y_{}_{}_{}'.format(job_id, resource_type, i))
                    solver.Add(x == interval.StartExpr())
                    box_vars[resource_type]['ivs'].append(interval)
                    box_vars[resource_type]['x'].append(x)
                    box_vars[resource_type]['dx'].append(dx)
                    box_vars[resource_type]['y'].append(y)
                    box_vars[resource_type]['dy'].append(dy)

                    y_var.append(y)
                    if resource_type == 'core':
                        decision_allocation_vars[job_id].append(y)
                    if not (i in elements):
                        elements[i] = []
                    e = solver.Element(resources_map[resource_type], y)
                    if not self._element_v2:
                        same_node = e == solver.Element(resources_map[resource_type], y + dy - 1)
                    else:
                        new_map = [
                            nvalue - resources_map[resource_type][i + requested_resources[resource_type] - 1] if i +
                                                                                                                 requested_resources[
                                                                                                                     resource_type] - 1 < len(
                                resources_map[resource_type]) else -1 for
                            i, nvalue
                            in enumerate(resources_map[resource_type])]
                        same_node = solver.Element(new_map, y) == 0
                    # Same node same resource
                    solver.Add(same_node)
                    elements[i].append((e, y))
                y_vars.append(y_var)
                if len(y_var) > 1:
                    if self._sim_break_alloc:
                        for i in range(len(y_var) - 1):
                            solver.Add(y_var[i] < y_var[i + 1])
                    else:
                        cst = solver.AllDifferent(y_var)
                        solver.Add(cst)

            jobs_allocations[job_id] = list(chain(*y_vars))

            # Same node among diff resources
            for n in elements:
                for i in range(len(elements[n])):
                    if i < len(elements[n]) - 1:
                        cst = elements[n][i][0] == elements[n][i + 1][0]
                        solver.Add(cst)

        for resource_type in resource_types:
            box_data = box_vars[resource_type]
            if not box_data['x']:
                continue
            xs = box_data['x']
            dxs = box_data['dx']
            ys = box_data['y']
            dys = box_data['dy']
            cst = solver.NonOverlappingBoxesWithSizesConstraint(xs, ys, dxs, dys, mks,
                                                                total_resource_capacities[resource_type], False)
            solver.Add(cst)

        all_job_ids = list(job_intervals_dict)
        for i in range(len(all_job_ids)):
            for j in range(len(all_job_ids)):
                if i < j:
                    job_id_i = all_job_ids[i]
                    job_id_j = all_job_ids[j]
                    job_i = es_dict[job_id_i]
                    job_j = es_dict[job_id_j]
                    if not (job_i.start_time and job_j.start_time):
                        energies_i = []
                        energies_j = []
                        nodes_energies = []
                        for r, cap in total_resource_capacities.items():
                            if (job_i.requested_resources[r] and job_j.requested_resources[r]):
                                energies_i.append(
                                    job_durations[job_id_i] * job_i.requested_nodes * job_i.requested_resources[r])
                                energies_j.append(
                                    job_durations[job_id_j] * job_j.requested_nodes * job_j.requested_resources[r])
                                nodes_energies.append(cap * max(job_durations[job_id_i], job_durations[job_id_j]))

                        if nodes_energies:
                            are_disjunctive = False

                            for e_i, e_j, e_n in zip(energies_i, energies_j, nodes_energies):
                                if e_i + e_j > e_n:
                                    are_disjunctive = True
                                    solver.Add(solver.DisjunctiveConstraint(
                                        [job_intervals_dict[job_id_i], job_intervals_dict[job_id_j]],
                                        'disjunctive_{}_{}'.format(job_id_i, job_id_j)))
                                    break
                            if not are_disjunctive and all([e_i <= e_j for e_i, e_j in zip(energies_i, energies_j)]):
                                solver.Add(start_vars[job_id_j] >= start_vars[job_id_i])

        slowdowns = []
        for job_var in decision_sched_vars:
            job_obj = es_dict[job_var.Name()]
            if not job_obj.start_time:
                current_waiting_time = (cur_time - job_obj.queued_time) + job_var.StartExpr()
                job_duration = max(1, job_obj.expected_duration)
                slowdown = ((current_waiting_time + job_duration) * 100000) // job_duration
                slowdowns.append(slowdown)

        monitors = []

        objective_var = solver.Sum(slowdowns).Var()

        composed_obj = objective_var

        objective_monitor = solver.Minimize(composed_obj, 1)
        monitors.append(objective_monitor)

        bestCollector = solver.BestValueSolutionCollector(False)
        bestCollector.AddObjective(objective_var)
        bestCollector.Add(decision_sched_vars)
        bestCollector.Add(box_vars['core']['y'])
        monitors.append(bestCollector)

        bestSolution = dict(score=sys.maxsize, timestamp=int(time() * 1000), retry=0, timelimit=self._initial_timelimit,
                            max_timelimit=max_timelimit, max_retries=self._max_retries)

        def searchLimit():
            if bestCollector.SolutionCount() > 0 and bestCollector.ObjectiveValue(0) < bestSolution['score']:
                bestSolution['score'] = bestCollector.ObjectiveValue(0)
            if bestCollector.SolutionCount() == 0:
                if int(time() * 1000) - bestSolution['timestamp'] > bestSolution['timelimit']:
                    if bestSolution['retry'] < bestSolution['max_retries']:
                        bestSolution['retry'] += 1
                        bestSolution['timelimit'] += 500
                        return False
                    else:
                        return True
            elif int(time() * 1000) - bestSolution['timestamp'] > bestSolution['timelimit']:
                return True
            return False

        limit_monitor = solver.CustomLimit(searchLimit)
        monitors.append(limit_monitor)

        sched_vars = decision_sched_vars
        alloc_vars = [jobs_allocations[interval.Name()] for interval in sched_vars]
        db = solver.InterleavedSearch(sched_vars, alloc_vars, self._sched_bt)

        solved = solver.Solve(db, monitors)

        dispatching_decision['solved'] = solved
        proc_time = int(time() * 1000)

        if solved:
            for var in decision_sched_vars:
                job_id = var.Name()
                if job_id in running_jobs:
                    continue
                if bestCollector.StartValue(0, var) == 0:
                    nodes_list = []
                    for position in decision_allocation_vars[job_id]:
                        nodes_list.append('node_{}'.format(resources_map['core'][bestCollector.Value(0, position)]))
                    dispatching_decision[job_id] = self.dispatching_tuple(job_id, cur_time, nodes_list)
                else:
                    dispatching_decision[job_id] = self.dispatching_tuple(job_id)
            dispatching_decision['of_value'] = bestCollector.ObjectiveValue(0)
            dispatching_decision['walltime'] = bestCollector.WallTime(0)
            dispatching_decision['failures'] = bestCollector.Failures(0)
            dispatching_decision['branches'] = bestCollector.Branches(0)
            dispatching_decision['proc_time'] = int(time() * 1000) - proc_time
        else:
            for job_id in priorized_jobs:
                dispatching_decision[job_id] = self.dispatching_tuple(job_id)
            dispatching_decision['of_value'] = -1
            dispatching_decision['walltime'] = -1
            dispatching_decision['failures'] = -1
            dispatching_decision['branches'] = -1
            dispatching_decision['proc_time'] = int(time() * 1000) - proc_time

        solver.EndSearch()
        solver = None
        return solved
