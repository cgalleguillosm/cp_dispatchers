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

from accasim.base.scheduler_class import SchedulerBase
from ortools.constraint_solver import pywrapcp
from enum import Enum
from sortedcontainers.sortedlist import SortedListWithKey, SortedList
from itertools import groupby
import sys

class PCP1(SchedulerBase):
    """
    A scheduler which uses constraint programming to plan a sub-optimal schedule.
    This scheduler don't use the automatic feature of the automatic allocation, then it isn't

    """
    name = 'PCP1'
        
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
        self.use_max_timelimit = kwargs.pop('use_max_timelimit', False)
        self.debug = True if kwargs.pop('debug', 'INFO') != 'INFO' else False
        self.cur_q_length = self.q_length = kwargs.pop('q_length', 100)
        self.reduce_job_length = kwargs.pop('reduce_job_length', True)
        self.timelimit = kwargs.pop('timelimit', 16000)
        self.initial_timelimit = kwargs.pop('initial_timelimit', 1000)
        self.significant = kwargs.pop('significant', 5)           
        self.max_k = kwargs.pop('max_k', 2)     
                        
        # Job dispatching skip 
        self.non_dispatched_state = None
            
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
                    
        #=======================================================================
        # Considered queued jobs: Jobs can be fitted in the current system state and less or equal than q_length
        # If a job_obj cannot be fitted or exceed the q_length is directly loaded in the dispatching decision using the no-solution dispatching tuple 
        #=======================================================================
        priorized_jobs = SortedListWithKey(key=lambda job_tuple: job_tuple[1])
              
        current_qjobs = SortedList() 

        #===================================================================
        # Here, if there is a non dispatching previous state set, the current system capacity
        # is verified if it is different (more reasource available than before) the dispatcher is called.
        # Otherwise, a non dispatching decision is returned.
        #===================================================================
        
        # Dispatching Skip
        dispatch = True
        prev_qjobs = None
        
        # Dispatching skip
        if self.non_dispatched_state:

            dispatch = False
            (prev_qjobs, prev_total_resource_usage,) = self.non_dispatched_state
            
            new_jobs = False

            for e in es:                    
                if not(e.id in prev_qjobs):
                    new_jobs = True
                    self.non_dispatched_state = None
                    break        
            if not new_jobs:
                
                cur_total_resource_usage = self.resource_manager._resources.usage('dict')
                
                zero_usage = []
                same_usage = []
                for res in resource_types:
                    zero_usage.append(cur_total_resource_usage[res] == 0)
                    same_usage.append(cur_total_resource_usage[res] >= prev_total_resource_usage[res])                    
                
                if all(zero_usage):
                    # The system is empty
                    self.non_dispatched_state = None
                elif all(same_usage):
                    # The system has the same or less capacity wrt the stuck state
                    return [self.dispatching_tuple(e.id) for e in es], []
                else:
                    # The system is not empty but has more capacity wrt the stuck state
                    self.non_dispatched_state = None
                            
        cons_qjobs = {}
        max_ewt = max([self.get_ewt(job.queue) for job in es] + [self.get_ewt(es_dict[job_id]) for job_id in self.resource_manager.current_allocations])
        for node in self.resource_manager.node_names:
            avl_res = avl_resources[node]
            for idx, job_obj in enumerate(es):
                job_id = job_obj.id
                
                if not(job_id in cons_qjobs):
                    current_qjobs.add(job_id)
                    cons_qjobs[job_id] = [False, 0, {}, None]
                    priorized_jobs.add((job_id, self._job_priority_ewt(job_obj, cur_time, max_ewt)))
                        
                possibilities = self._joint_nodes(job_obj, avl_res)
                if possibilities > 0:
                    cons_qjobs[job_id][2][node] = min(possibilities, job_obj.requested_nodes)
                    cons_qjobs[job_id][1] += possibilities 
                    if cons_qjobs[job_id][1] >= job_obj.requested_nodes: 
                        cons_qjobs[job_id][0] = True
                        if not cons_qjobs[job_id][3]: 
                            cons_qjobs[job_id][3] = job_obj
                                                       
        qjobs = 0
        wc_makespan = 0
        makespans = []
        
        remaining_priorized_jobs = []
        
        # Job of the dispatching decision 
        decision_jobs = {}    
        
        for job_id, _ in priorized_jobs:
            t = cons_qjobs[job_id]
            if not t[0] or qjobs > self.cur_q_length - 1:
                decision_jobs[job_id] = self.dispatching_tuple(job_id)
                cons_qjobs.pop(job_id)
            else:
                exp_duration = t[-1].expected_duration
                wc_makespan += exp_duration
                makespans.append(exp_duration)
                qjobs += 1
                remaining_priorized_jobs.append(job_id)
        #=======================================================================
        # There are no jobs to dispatch at the current system state. 
        # Then a no solution list is returned. 
        #=======================================================================
        if not cons_qjobs:           
            # Job Dispatching skip
            cur_total_resource_usage = self.resource_manager._resources.usage('dict')
            self.non_dispatched_state = (current_qjobs, cur_total_resource_usage,)
            
            return decision_jobs.values(), []

        #=======================================================================
        # After an unsuccessful dispatching
        #=======================================================================
        if self.use_max_timelimit:
            timelimit = self.timelimit
        else: 
            timelimit = self.initial_timelimit
                
        a_jobs_list = []
        best_z_list = []
        solved = False
        
        self.priorized_jobs = None
        
        prev_sched = []
        while timelimit <= self.timelimit:
            schedalloc_plan = {}
            args = (schedalloc_plan, cur_time, cons_qjobs, remaining_priorized_jobs, es_dict, resource_types, avl_resources)
            kwargs = {'timelimit':timelimit, 'prev_sched':prev_sched}
            function = getattr(self, 'cp_model')
            function(*args, **kwargs)

            solver_state = schedalloc_plan.pop('solver_state')
            best_z = schedalloc_plan.pop('best_z')
            best_z_list.append(best_z)
            
            if solver_state == self.SolverState.PROBLEM_INFEASIBLE:
                break
            limit_reached = schedalloc_plan.pop('limit_reached')            
            
            disp_jobs = 0
            prev_sched = [] 
            for stime, job_id, _ in schedalloc_plan.values():
                if stime == cur_time:
                    prev_sched.append(job_id)
                    disp_jobs += 1
                
            if disp_jobs == len(cons_qjobs) and solver_state == self.SolverState.NO_MORE_SOLUTIONS.value and not limit_reached:
                solved = True
                break
            elif disp_jobs < len(cons_qjobs) and solver_state == self.SolverState.NO_MORE_SOLUTIONS.value and not limit_reached:
                solved = True
                break
            elif len(best_z_list) >= self.max_k and all([best_z_list[-1] == b for b in best_z_list[-self.max_k:]]):
                solved = True
                break
            else:
                a_jobs_list.append(disp_jobs)
                timelimit *= 2 
        
        self.priorized_jobs = None
        
        # This is useful for print and also to create the unsuccessful data
        dispatched_jobs = 0
        queued_job_ids = []
        for a in schedalloc_plan:
            if a[2]:
                dispatched_jobs += 1
            if dispatched_jobs == 0:
                queued_job_ids.append(a[1])

        if self.reduce_job_length:
            #===================================================================
            # The considered number of jobs in the next scheduling decision are reduced to the half
            # if the current problem instance was not solved, if the current usage is
            # leq of the previous time point. After a successful dispatching this value is reset. 
            # The minimum is 1, otherwise there will be nothing to dispatch
            #===================================================================
            if not solved:
                self.cur_q_length = max(1, self.cur_q_length // 2)
            else:
                self.cur_q_length = self.q_length
        if dispatched_jobs == 0:
            self.non_dispatched_state = (current_qjobs, self.resource_manager._resources.usage('dict'),)
        else:
            self.non_dispatched_state = None
                 
        return list(schedalloc_plan.values()) + list(decision_jobs.values()), []
    
    def cp_model(self, decision_jobs, cur_time, cons_qjobs, priorized_jobs, es_dict, resource_types, avl_resources, timelimit=15000, debug=False, prev_sched=[]):
        solver = pywrapcp.Solver('PCP1')

        makespans = []
        wc_makespan = 0

        job_vars = {}
        #=======================================================================
        # Running jobs are considered into the model to give more realistic schedules using the expected durations.
        # Running jobs have their min and max start time equals to 0
        #=======================================================================
        running_jobs = self.resource_manager.current_allocations
        running_jobs_map = {}
        
        for job_id in running_jobs:
            job_obj = es_dict[job_id]
            remaining_duration = job_obj.expected_duration - (cur_time - job_obj.start_time)
            
            wc_makespan += remaining_duration
            makespans.append(remaining_duration)
            
            interval = solver.FixedDurationIntervalVar(0, 0, remaining_duration, False, job_id)
            job_vars[job_id] = interval
            
            allocated_nodes = {k: len(list(v)) for k, v in groupby(job_obj.assigned_nodes)}
            running_jobs_map[job_id] = [True, job_obj.requested_nodes, allocated_nodes, job_obj]
            
        #=======================================================================
        # Add the expected duration of queued jobs to the list of makespans
        #=======================================================================
        for job_id, prop in cons_qjobs.items():
            job_obj = prop[-1]
            expected_duration = job_obj.expected_duration
            wc_makespan += expected_duration 
            makespans.append(expected_duration)
            
        #=======================================================================
        # cons_qjobs var are converted into IntervalVars for the CP model
        # Current time is represented as 0 in the model
        #  if a exp_duration is 0, its considered as 1 (a 0 exp_duration isn't valid for a FixedDurationInvertalVar)
        #=======================================================================
        # for job_id, t in cons_qjobs.items():
        for job_id in priorized_jobs:
            t = cons_qjobs[job_id]
            job_obj = t[-1]
            min_start = 0
            max_start = wc_makespan  # The model now forces only to start jobs at current the time 
            exp_duration = max(1, job_obj.expected_duration)
            optional = False  # All jobs are mandatory
            interval = solver.FixedDurationIntervalVar(min_start, max_start, exp_duration, optional, job_id)
            job_vars[job_id] = interval
                
        utils_nodes = {}
        
        # Allocation alternatives for queued jobs
        not_included = []
        current_jobs = {**running_jobs_map, **{job_id: cons_qjobs[job_id] for job_id in priorized_jobs}}
        for job_id, t in current_jobs.items():
            job_obj = t[-1]
            min_start = 0
            if job_obj.start_time:
                expected_duration = job_obj.expected_duration - (cur_time - job_obj.start_time)
                max_start = 0
                optional = False
            else:
                expected_duration = max(1, job_obj.expected_duration)
                optional = True
                max_start = wc_makespan  # The model now forces only to start jobs at current the time 

            utils_nodes[job_id] = {}
            for node, node_possibilities in t[2].items():
                utils_nodes[job_id][node] = []
                for f in range(node_possibilities):
                    util_name = 'utilnode_{}_{}_{}'.format(job_id, node, f)
                    interval = solver.FixedDurationIntervalVar(min_start, max_start, expected_duration, optional, util_name)
                    utils_nodes[job_id][node].append(interval)

        #===========================================================================
        # Alternative constraint for each job_obj. 
        # outerInterval: outer interval (interval under study)
        # innerInterval:  list of inner intervals (possibilities of selection) 
        # subUnitsNumber: required selections         
        #===========================================================================
        obj_intervals = {}
        for job_id, interval in job_vars.items():
            job_obj = current_jobs[job_id][-1]
            inner_intervals = [ f for node, fit in utils_nodes[job_id].items() for f in fit]
            obj_intervals[job_id] = inner_intervals
            if len(inner_intervals) >= job_obj.requested_nodes:
                alt_cst = solver.Alternative(interval, inner_intervals, job_obj.requested_nodes)
                solver.Add(alt_cst)
            else:
                not_included.append(job_id)
                job_vars.pop(job_id)
                obj_intervals.pop(job_id)
        
        all_intervals = []    
        for job_id in priorized_jobs:
            if job_id in not_included:
                decision_jobs[job_id] = self.dispatching_tuple(job_id)
            else:
                for node, alloc_alt in utils_nodes[job_id].items():
                    all_intervals.extend(alloc_alt)
        #======================================================================
        # Cumulative constraint
        # One cumulative constraint for each node and resource type. (64 nodes x 4 resource types)
        #
        # This constraint forces that, for any integer t, the sum of the demands corresponding to an interval 
        # containing t does not exceed the given capacity. Intervals and demands should be vectors of equal size. 
        # Demands should only contain non-negative values. Zero values are supported, and the corresponding intervals 
        # are filtered out, as they neither impact nor are impacted by this constraint.
        # Arguments: 
        #      intervals: Interval (Array of interval vars)
        #      demands: Array corresponding to the invterval demands (array of ints) 
        #      capacity: System capacity (int)
        #      name: Name of the constraint (str) 
        #======================================================================           
        nodes_capacities = self.resource_manager.system_capacity('nodes')
        for res_type in resource_types:
            for node in self.resource_manager.node_names:
                node_capacity = nodes_capacities[node]

                intervals = []
                demands = []
                cum_name = 'cum_{}_{}'.format(node, res_type)
                for job_id, job_model in job_vars.items():
                    job_obj = current_jobs[job_id][-1]

                    if not(node in utils_nodes[job_id]):
                        continue
                    job_demand = job_obj.requested_resources[res_type]
                    if job_demand == 0:
                        continue
                    possibilities = utils_nodes[job_id][node]
                    intervals += possibilities
                    demands += [job_demand] * len(possibilities)
                if sum(demands) > 0:
                    cum_cst = solver.Cumulative(intervals, demands, node_capacity[res_type], cum_name)
                    solver.Add(cum_cst)

        #=======================================================================
        # Objective Function
        #=======================================================================
        ewts = []
        
        digits = max(self.significant, len(str(wc_makespan)))
        
        for job_id, job_model in job_vars.items():
            job_obj = current_jobs[job_id][-1]
            ewt = self.get_ewt(job_obj.queue)
            cur_waiting_time = (cur_time - job_obj.queued_time)
            job_start = solver.Min([interval.SafeStartExpr(wc_makespan - job_obj.expected_duration) for interval in obj_intervals[job_id]])
            ewt_job = solver.Max(((job_start + cur_waiting_time - ewt) * (10 ** digits)) // ewt, solver.IntConst(0))
            ewts.append(ewt_job)

        monitors = []

        objective_var = solver.Sum(ewts).Var()
        objective_monitor = solver.Minimize(objective_var, 1)
        monitors.append(objective_monitor)
        
        #=======================================================================
        # Search
        #=======================================================================
        db = solver.HeuristicSearch(all_intervals)

        #=======================================================================
        # Other utilities
        #=======================================================================
        limit_monitor = solver.TimeLimit(timelimit)
        monitors.append(limit_monitor)
        
        best_monitor = solver.BestValueSolutionCollector(False)
        best_monitor.AddObjective(objective_var)
        best_monitor.Add(list(job_vars.values()))
        best_monitor.Add(all_intervals)
        monitors.append(best_monitor)
               
        solver.NewSearch(db, monitors)

        solved  = False
        while solver.NextSolution():
            solved = True
            solution = {}
            for job_id, job_interval in job_vars.items():
                if job_id in running_jobs:
                    continue
                if job_interval.MustBePerformed() and job_interval.StartMin() == 0:
                    nodes = []
                    for node_id, job_units in utils_nodes[job_id].items():
                        for node in job_units:
                            if node.MustBePerformed():
                                nodes.append(node_id)
                    if nodes:
                        if es_dict[job_id].requested_nodes == len(nodes):
                            solution[job_id] = self.dispatching_tuple(job_id, cur_time, nodes)
                        else:
                            raise Exception('Requested {} . Nodes {}'.format(es_dict[job_id].requested_nodes, nodes)) 
                    else:
                        solution[job_id] = self.dispatching_tuple(job_id)
                else:
                    solution[job_id] = self.dispatching_tuple(job_id)
            decision_jobs['best_z'] = objective_var.Value()

        solver.EndSearch()
        exec_time = solver.WallTime()
        decision_jobs['solver_state'] = 4 if exec_time < timelimit and solved else solver.State()# solver.State()
        solver = None        
        decision_jobs['limit_reached'] = exec_time >= timelimit  # Should be == but it could take an extra ms to stop the solver. 

        if not solved:
            for job_id in cons_qjobs:
                decision_jobs[job_id] = self.dispatching_tuple(job_id)
                decision_jobs['best_z'] = sys.maxsize
        else:
            for job_id, tuple in solution.items():
                decision_jobs[job_id] = tuple       
        return solved
    
    def get_id(self):
        """
        Returns the full ID of the scheduler, including policy and allocator.

        :return: the scheduler's id.
        """
        return self.name
            
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

    def _job_priority_ewt(self, job_obj, cur_time, max_ewt):
        return (
        -(max_ewt * cur_time - job_obj.queued_time + 1) / self.get_ewt(job_obj.queue),
        job_obj.expected_duration * sum([job_obj.requested_nodes * val for attr, val in job_obj.requested_resources.items()])
        )

    def get_ewt(self, queue_type):
        """
        @param queue_type: Name of the queue.

        @return: EWT for a specific queue type
        """
        if queue_type in self.ewt:
            return self.ewt[queue_type]
        return self.ewt['default']
