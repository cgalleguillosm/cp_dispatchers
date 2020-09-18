import sys
from os.path import abspath, join
from sortedcontainers.sortedlist import SortedListWithKey, SortedList
from ortools.constraint_solver import pywrapcp
from time import time
from json import dumps
from itertools import groupby
from math import gcd as _gcd

accasim = abspath(join('../../../accasim'))
sys.path.insert(0, accasim)

from abc import abstractmethod
from accasim.base.scheduler_class import SchedulerBase


class Dispatcher(SchedulerBase):
    """
    PCP20 Dispatcher

    """
    name = 'PCP20'

        
    def __init__(self, resource_manager=None, seed=0, search='default', safe=False, **kwargs):
        SchedulerBase.__init__(self, seed, None)
        
        self._safe = kwargs.pop('safe', False)        
        self._debug = kwargs.pop('debug', False)
        self._cur_q_length = self._q_length = kwargs.pop('q_length', 100)        
        self._reduce_job_length = kwargs.pop('reduce_job_length', True) 
        self._max_timelimit = kwargs.pop('timelimit', 16000)
        # Job dispatching skip 
        self._skip_same_states = kwargs.pop('skip_same_states', False) 
        self._non_dispatched_state = None
        self._reduced_model = kwargs.pop('reduced_model', True)
        self._considered_cannot_start = kwargs.pop('considered', 1)
        self._print_instances = kwargs.pop('print_instances', False)
        self._max_retries = kwargs.pop('max_retries', 4)
        self._trace_search = kwargs.pop('trace_search', False)
        self._trace_propagation = kwargs.pop('trace_propagation', False)
        self._interval = kwargs.pop('interval_search', True)
        self._v2 = kwargs.pop('v2', False)
        self._pack_jobs = kwargs.pop('pack_jobs', False)
        self._independent_x = kwargs.pop('independent_x', False)
        self._bounded_boxes = kwargs.pop('bounded_boxes', False)
                
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
        
        if self._pack_jobs and not hasattr(self, "generic_request"):
            self._min_max_generic_request()        
            
        if self._pack_jobs:
            for job_obj in es:
                # job_obj = es_dict[e]
                if not hasattr(job_obj, "request_updated"):
                    # raise('Error on SWF requests...')
                    #===========================================================
                    # for r in resource_types:
                    #     print(self.generic_request[r] , job_obj.requested_nodes * job_obj.requested_resources[r])
                    #===========================================================
                    gcd = min([_gcd(self.generic_request[r] , job_obj.requested_nodes * job_obj.requested_resources[r]) for r in resource_types if job_obj.requested_resources[r] > 0 ])                
                    job_obj.override_requested_nodes(job_obj.requested_nodes // gcd) 
                    for r in resource_types:
                        # job_obj.requested_resources[r] *= _gcd
                        job_obj.override_requested_resources(r, job_obj.requested_resources[r] * gcd)
                    job_obj.request_updated = True
        
        if self._skip_same_states and self._non_dispatched_state:

            dispatch = False
            (prev_qjobs, prev_total_resource_usage,) = self._non_dispatched_state
            
            new_jobs = False

            for e in es:                    
                if not(e.id in prev_qjobs):
                    new_jobs = True
                    self._non_dispatched_state = None
                    break        
            if not new_jobs:
                if self.debug:
                    print('There is no new jobs')
                
                cur_total_resource_usage = self.resource_manager._resources.usage('dict')
                
                zero_usage = []
                same_usage = []
                for res in resource_types:
                    zero_usage.append(cur_total_resource_usage[res] == 0)
                    same_usage.append(cur_total_resource_usage[res] >= prev_total_resource_usage[res])                    
                
                if all(zero_usage):
                    # The system is empty
                    self._non_dispatched_state = None
                elif all(same_usage):
                    # The system has the same or less capacity wrt the stuck state
                    if self.debug:
                        print('{}: The system has the same or less capacity wrt the stuck state, skipping the current dispatching decision'.format(cur_time))
                            # print(self.resource_manager.current_usage)
                        print('Current: ', cur_total_resource_usage)
                        print('Previous: ', prev_total_resource_usage)
                    return [self.dispatching_tuple(e.id) for e in es], []
                else:
                    # The system is not empty but has more capacity wrt the stuck state
                    self._non_dispatched_state = None
        
        #=======================================================================
        # Considered queued jobs: Jobs can be fitted in the current system state and less or equal than q_length
        # If a job_obj cannot be fitted or exceed the q_length is directly loaded in the dispatching decision using the no-solution dispatching tuple 
        #=======================================================================
        priorized_jobs = SortedListWithKey(key=lambda job_tuple: job_tuple[1])
              
        current_qjobs = SortedList() 
                            
        cons_qjobs = {}
        for node in self.resource_manager.node_names:
            avl_res = avl_resources[node]
            # avl_res = system_capacity[node]
            for idx, job_obj in enumerate(es):
                job_id = job_obj.id
                
                if not(job_id in cons_qjobs):
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
                    wc_makespan += exp_duration  # , self.get_queue(t[-1].queue))  # exp_duration
                    makespans.append(exp_duration)
                    qjobs += 1
                    selected_priorized_jobs.append(job_id)
        else:
            cannot_start_selected = 0
            for job_id, _ in priorized_jobs:
                t = cons_qjobs[job_id]
                if (not t[0] and cannot_start_selected >= self._considered_cannot_start) or (qjobs > self._cur_q_length - 1):
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
        #=======================================================================
        # There are no jobs to dispatch at the current system state. 
        # Then a no solution list is returned. 
        #=======================================================================
        if not cons_qjobs:
            if self._debug:
                print('{}: There are no jobs to fit in the current system state.'.format(self._counter))
            
            # Job Dispatching skip
            cur_total_resource_usage = self.resource_manager._resources.usage('dict')
            self._non_dispatched_state = (current_qjobs, cur_total_resource_usage,)
            
            return decision_jobs.values(), []

        solved = False      
        self.priorized_jobs = None
        stime = time()
        if self._safe:
            manager = mp_dill.Manager()
            schedule_plan = manager.dict()
            process_class = mp_dill.Process
            
            p = process_class(target=getattr(self, 'cp_model'),
                args=(schedule_plan, cur_time, cons_qjobs, selected_priorized_jobs, es_dict, resource_types, avl_resources),
                # kwargs={'solution_rate':solution_rate, 'timelimit':timelimit, 'debug': self.debug or debug, 'jobs_priority': remaining_priorized_jobs}
                kwargs={'timelimit':timelimit}
            ) 
            p.start()
            p.join()
                
            if p.exitcode != 0:  # and not self.original:
                schedule_plan.pop('solver_state', None)
                schedule_plan.pop('limit_reached', None)
                return list(decision_jobs.values())    \
                    +[self.dispatching_tuple(job_id, start_time, nodes) for (start_time, job_id, nodes) in schedule_plan.values()]    \
                    +[self.dispatching_tuple(job_id, None, []) for job_id in cons_qjobs if not (job_id in schedule_plan)], []
        else:
            schedule_plan = {}
            args = (schedule_plan, cur_time, cons_qjobs, selected_priorized_jobs, es_dict, resource_types, avl_resources)
            kwargs = {'max_timelimit':self._max_timelimit}
            function = getattr(self, 'cp_model')
            function(*args, **kwargs)

        # solver_state = schedule_plan.pop('solver_state')
        # limit_reached = schedule_plan.pop('limit_reached')
        solved = schedule_plan.pop('solved')
        of_value = schedule_plan.pop('of_value')
        walltime = schedule_plan.pop('walltime')
        proc_time = schedule_plan.pop('proc_time')
        # incurred_time = time() - stime
        incurred_time = walltime + proc_time
        failures = schedule_plan.pop('failures')
        branches = schedule_plan.pop('branches')
        p = None
        
        self.priorized_jobs = None
        dispatching_plan = list(schedule_plan.values())
        self.__instance_data = (solved, of_value, walltime, incurred_time, failures, branches, dispatching_plan + list(decision_jobs.values()),)
                
        # This is useful for print and also to create the unsuccessful data
        dispatched_jobs = 0
        queued_job_ids = []
        for a in dispatching_plan:
            if a[2]:
                dispatched_jobs += 1
            if dispatched_jobs == 0:
                queued_job_ids.append(a[1])

        if self._reduce_job_length:
            #===================================================================
            # The considered number of jobs in the next scheduling decision are reduced to the half
            # if the current problem instance was not solved, if the current usage is
            # leq of the previous time point. After a successful dispatching this value is reset. 
            # The minimum is 1, otherwise there will be nothing to dispatch
            #===================================================================
            if not solved:
                # self._cur_q_length = max(1, self._cur_q_length // 2)
                self._cur_q_length = max(1, min(self._cur_q_length, len(schedule_plan)) // 2)
            else:
                self._cur_q_length = self._q_length
        
        if self._skip_same_states:
            if dispatched_jobs == 0:
                self._non_dispatched_state = (current_qjobs, self.resource_manager._resources.usage('dict'),)
            else:
                self._non_dispatched_state = None
                 
        print('{} - {}: Queued {}, Dispatched {}, Running {}. {}'.format(self._counter, cur_time, len(es) - dispatched_jobs, dispatched_jobs, len(self.resource_manager.current_allocations), self.resource_manager.current_usage))
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
        job_obj.expected_duration * sum([job_obj.requested_nodes * val for attr, val in job_obj.requested_resources.items()])
        )
    
    def dispatching_tuple(self, job_id, time=None, allocation=[]):
        # assert(isinstance(job_id, str)), 'For job_id only str is accepted'
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

    def get_instance_data(self):
        tmp = self.__instance_data
        self.__instance_data = None
        return tmp
    
    def _min_max_generic_request(self):
        """
            Finds the max-min node type for a for a generic request.             
        """
        system_def = [d['resources'] for d in self.resource_manager._resources._definition]
        system_res_types = self.resource_manager.system_resource_types()
        self.generic_request = { res: min([group.get(res, 0) for group in system_def]) for res in system_res_types}
        return        

    
class PCP20Dispatcher(Dispatcher):
    
    def job_alternatives(self, avl_resources, job):
        alternatives = {}
        total_alternatives = 0
        for node, resources in avl_resources.items():
            times = min(min([capacity // job.requested_resources[resource] for (resource, capacity) in resources.items() if job.requested_resources[resource] > 0]), job.requested_nodes)
            alternatives[node] = times
            total_alternatives += times
        return total_alternatives, alternatives

    def cp_model(self, dispatching_decision, cur_time, cons_qjobs, priorized_jobs, es_dict, resource_types,
                   avl_resources, max_timelimit=15000, debug=False):
        resource_types = self.resource_manager.resource_types
        avl_resources = self.resource_manager.current_availability  # Avl resources (with consumed resources)
        nodes_capacity = self.resource_manager.system_capacity('nodes')  # Avl resources (without consumed resources)
        total_resources_capacities = [0] * len(resource_types)
        max_resources_capacities = [0] * len(resource_types)
        for i, r in enumerate(resource_types):
            for cap in nodes_capacity.values():
                total_resources_capacities[i] += cap[r]
                max_resources_capacities[i] = max(max_resources_capacities[i], cap[r])

        parameters = pywrapcp.Solver_DefaultSolverParameters()
        parameters.trace_search = self._trace_search
        parameters.trace_propagation = self._trace_propagation
        solver = pywrapcp.Solver('composed', parameters)

        # Makespan of queued jobs
        mks = sum([max(1, t[3].expected_duration) for t in cons_qjobs.values()])

        # Stores a dictionary of nodes and a dictionary of corresponding job usage on the node alloc_var <- [node][job_id]
        system_allocations = {node: {} for node in nodes_capacity}
        # Stores the IntervalVar corresponding to each job
        vars_dict = {}
        # Stores allocation variables of jobs list of alloc_var <-[job_id]
        jobs_allocations = {}
        jobs_nodes_allocations = {}
        # Stores the nodes on which a job can be allocates
        jobs_current_alternatives = {}

        # Runing jobs
        dys = {}
        start_vars = {}
        start_vars_array = []
        jobs_duration = {}
        jobs_duration_array = []
        running_jobs = self.resource_manager.current_allocations
        all_jobs = []
        jobs_requests = []
        for job_id in running_jobs:
            job = es_dict[job_id]
            dys[job_id] = {resource_type: [] for resource_type in resource_types}
            expected_rem_duration = max(1, job.expected_duration - (cur_time - job.start_time))
            jobs_duration[job_id] = expected_rem_duration
            jobs_duration_array.append(expected_rem_duration)
            # start_var = solver.IntVar(0, mks)
            start_var = solver.IntConst(0, 'start_{}'.format(job_id))
            start_vars[job_id] = start_var
            start_vars_array.append(start_var)
            solver.Add(start_var == 0)
            job_var = solver.FixedDurationIntervalVar(start_var, expected_rem_duration, str(job_id))

            all_jobs.append(job_var)
            jobs_requests.append([job.requested_nodes * job.requested_resources[resource_type] for resource_type in resource_types])
            mks += expected_rem_duration

            _, job_alternatives = self.job_alternatives(running_jobs[job_id], job)

            allocations_vars = []
            for node in nodes_capacity:
                allocs = job_alternatives.get(node, 0)
                alloc_var = solver.IntVar(allocs, allocs, '{}_{}'.format(job_id, node))
                system_allocations[node][job_id] = alloc_var
                allocations_vars.append(alloc_var)
            vars_dict[job_id] = job_var
            jobs_allocations[job_id] = allocations_vars
            cons_qjobs[job_id] = (None, None, None, job)

        decision_sched_vars = []
        decision_allocation_vars = []

        for job_id in priorized_jobs:
            job = cons_qjobs[job_id][3]
            dys[job_id] = {resource_type: [] for resource_type in resource_types}
            jobs_nodes_allocations[job_id] = {}

            job_duration = max(1, job.expected_duration)

            _, job_alternatives = self.job_alternatives(nodes_capacity, job)
            _, current_alternatives = self.job_alternatives(avl_resources, job)
            jobs_current_alternatives[job_id] = current_alternatives

            start_var = solver.IntVar(0, mks, 'start_{}'.format(job_id))
            start_vars[job_id] = start_var
            start_vars_array.append(start_var)
            jobs_duration[job_id] = job_duration
            jobs_duration_array.append(job_duration)
            job_var = solver.FixedDurationIntervalVar(start_var, job_duration, str(job_id))

            all_jobs.append(job_var)
            jobs_requests.append(
                [job.requested_nodes * job.requested_resources[resource_type] for resource_type in resource_types])
            allocations_vars = []
            for node, allocs in job_alternatives.items():
                # if allocs > 0:
                alloc_var = solver.IntVar(0, min(allocs, job.requested_nodes), '{}_{}'.format(job_id, node))
                system_allocations[node][job_id] = alloc_var
                allocations_vars.append(alloc_var)
                jobs_nodes_allocations[job_id][node] = alloc_var

            vars_dict[job_id] = job_var
            jobs_allocations[job_id] = allocations_vars
            decision_sched_vars.append(job_var)
            decision_allocation_vars += allocations_vars

            # Requested nodes
            cst = solver.SumEquality(allocations_vars, job.requested_nodes)
            solver.Add(cst)

        for r in range(len(total_resources_capacities)):
            sub_req = []
            sub_jobs = []
            sub_starts = []
            sub_durations = []
            for sjob, sstart, jreq, jdur in zip(all_jobs, start_vars_array, jobs_requests, jobs_duration_array):
                if jreq[r] > 0:
                    sub_jobs.append(sjob)
                    sub_starts.append(sstart)
                    sub_req.append(jreq[r])
                    sub_durations.append(jdur)
            cst = solver.Cumulative(sub_jobs, sub_req, total_resources_capacities[r], 'Cumulative_{}'.format(resource_types[r]))
            solver.Add(cst)
            cst = solver.Cumulative(
                [solver.FixedDurationIntervalVar(0, total_resources_capacities[r], jreq, False, 'job_{}_inverted'.format(i)) for i, jreq in enumerate(sub_req)],
                sub_starts, mks, 'Inverted_Cumulative_{}'.format(resource_types[r])
            )
            solver.Add(cst)

        all_job_ids = list(running_jobs) + priorized_jobs

        for i in range(len(all_job_ids)):
            for j in range(len(all_job_ids)):
                if i < j:
                    job_id_i = all_job_ids[i]
                    job_id_j = all_job_ids[j]
                    job_i = es_dict[job_id_i]
                    job_j = es_dict[job_id_j]
                    if not(job_i.start_time and job_j.start_time):
                        energies_i = []
                        energies_j = []
                        nodes_energies = []
                        for r, cap in zip(resource_types, total_resources_capacities):
                            if (job_i.requested_resources[r] and job_j.requested_resources[r]):
                                energies_i.append(jobs_duration[job_id_i] * job_i.requested_nodes * job_i.requested_resources[r])
                                energies_j.append(jobs_duration[job_id_j] * job_j.requested_nodes * job_j.requested_resources[r])
                                nodes_energies.append(cap * max(jobs_duration[job_id_i], jobs_duration[job_id_j]))

                        if nodes_energies:
                            are_disjunctive = False

                            for e_i, e_j, e_n in zip(energies_i, energies_j, nodes_energies):
                                if e_i + e_j > e_n:
                                    are_disjunctive = True
                                    solver.Add(solver.DisjunctiveConstraint([vars_dict[job_id_i], vars_dict[job_id_j]], 'disjunctive_{}_{}'.format(job_id_i, job_id_j)))
                                    break
                            if not are_disjunctive and all([e_i <= e_j for e_i, e_j in zip(energies_i, energies_j)]):
                                solver.Add(start_vars[job_id_j] >= start_vars[job_id_i])

        if self._independent_x:
            all_starts = {job_id: [start] for job_id, start in start_vars.items()}
        for node, capacities in nodes_capacity.items():

            for resource_type, resource_capacity in capacities.items():
                x = []
                y = []
                dx = []
                dy = []

                all_bounded = True
                for job_id, job_node_alloc in system_allocations[node].items():
                    if job_node_alloc.Max() == 0:
                        continue
                    job_obj = es_dict[job_id]
                    requested_resources = job_obj.requested_resources[resource_type]
                    if requested_resources == 0:
                        continue
                    if self._independent_x:
                        start_var = start_vars.get(job_id)
                        _x = solver.IntVar(start_var.Min(), start_var.Max(), 'start_{}_{}_{}'.format(job_id, node, resource_type))
                        all_starts[job_id].append(_x)
                    else:
                        _x = start_vars.get(job_id)
                    _dx = solver.IntConst(jobs_duration[job_id])
                    _y = solver.IntVar(0, resource_capacity, '{}_{}_{}'.format(job_id, node, resource_type))
                    _dy = (job_node_alloc * requested_resources)

                    if not _dy.Bound():
                        all_bounded = False

                    x.append(_x)
                    dx.append(_dx)
                    y.append(_y)
                    dy.append(_dy)
                    dys[job_id][resource_type].append(_dy)

                if dy and not all_bounded:
                    cst = solver.NonOverlappingBoxesWithSizesConstraint(x, y, dx, dy, mks, resource_capacity, self._bounded_boxes)
                    solver.Add(cst)
            
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
        bestCollector.Add(decision_allocation_vars)

        monitors.append(bestCollector)

        bestSolution = dict(score=sys.maxsize, timestamp=int(time() * 1000), retry=0, timelimit=1000,
                            max_timelimit=max_timelimit, max_retries=self._max_retries)

        def searchLimit():
            if bestCollector.SolutionCount() > 0 and bestCollector.ObjectiveValue(0) < bestSolution['score']:
                bestSolution['score'] = bestCollector.ObjectiveValue(0)

            if bestCollector.SolutionCount() == 0:
                if int(time() * 1000) - bestSolution['timestamp'] > bestSolution['timelimit']:
                    if bestSolution['retry'] < bestSolution['max_retries']:
                        # print(1, bestSolution)
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

        try:
            db = self.search(solver, cur_time, priorized_jobs, list(running_jobs.keys()), cons_qjobs,
                             vars_dict, jobs_allocations, start_vars_array if not self._interval else None)
        except KeyError as e:
            for job_id in priorized_jobs:
                print(job_id in vars_dict, cons_qjobs[job_id])
            raise e
        
        solved = solver.Solve(db, monitors)
        dispatching_decision['solved'] = solved
        proc_time = int(time() * 1000)
        if solved:
            for var in decision_sched_vars:
                job_id = var.Name()
                if bestCollector.StartValue(0, var) == 0:
                    nodes_list = []
                    sum_nodes = 0
                    req_nodes = es_dict[job_id].requested_nodes
                    for node, alloc in jobs_nodes_allocations[job_id].items():
                        times_node = bestCollector.Value(0, alloc)
                        sum_nodes += times_node
                        if times_node > 0:
                            nodes_list += [node] * times_node
                            if sum_nodes == req_nodes:
                                break
                    dispatching_decision[job_id] = self.dispatching_tuple(job_id, cur_time, nodes_list)
                else:
                    dispatching_decision[job_id] = self.dispatching_tuple(job_id)

            dispatching_decision['of_value'] = bestCollector.ObjectiveValue(0)
            dispatching_decision['walltime'] = bestCollector.WallTime(0)
            dispatching_decision['failures'] = bestCollector.Failures(0)
            dispatching_decision['branches'] = bestCollector.Branches(0)
            dispatching_decision['proc_time'] = int(time() * 1000) - proc_time
            # print('END Prepare solution: ', time()*1000 - bestSolution['timestamp'])
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

    def search(self, solver, cur_time, priorized_jobs, running_jobs, cons_qjobs, decision_vars_dict, sysalloc_decision_dict, start_vars=None):
        if self._print_instances:
            for node, res in self.resource_manager.current_availability.items():
                print(node, res)
            for e in cons_qjobs:
                job = cons_qjobs[e][-1]
                if not job.start_time:
                    print(job.id, job.requested_nodes, job.requested_resources)
        
        sched_vars = []
        sysalloc_decisions_vars = []
        system_demands = []
        system_capacities = [ [ node_resources[res_type] for res_type in self.resource_manager.resource_types] for node, node_resources in self.resource_manager.system_capacity('nodes').items()]

        avl_resources = self.resource_manager.current_availability

        if self._print_instances:
            json_data = {
                'cur_time': cur_time,
                'initial_solution': {},
                'workload': [],
                'job_ids': []                
            }
            
        if self._print_instances:
            for job_id in running_jobs:
                job_obj = cons_qjobs[job_id][-1]
                json_data['initial_solution'][job_id] = {'start_time': job_obj.start_time,
                                                         'allocation': {k: len(list(v)) for k, v in
                                                                        groupby(job_obj.assigned_nodes)}}        
        # for job_id in running_jobs + priorized_jobs:
        for job_id in priorized_jobs:
            job_obj = cons_qjobs[job_id][-1]
            if self._print_instances:
                json_data['job_ids'].append(job_id)
            sched_vars.append(decision_vars_dict[job_id])
            sysalloc_decisions_vars.append(sysalloc_decision_dict[job_id])
        
        if self._print_instances:
            print(self._print_instances, self._counter, dumps(json_data))

        db = None
        if start_vars:
            db = solver.InterleavedSearchIntVar(
                start_vars,
                sysalloc_decisions_vars
            )
        else:
            db = solver.InterleavedSearch(
                sched_vars,
                sysalloc_decisions_vars,
                self._v2,
            )
        
        return db