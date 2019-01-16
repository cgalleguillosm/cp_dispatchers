"""
MIT License

Copyright (c) 2017 cgalleguillosm

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

class HCP3(SchedulerBase):
    """
    A scheduler which uses constraint programming to plan a sub-optimal schedule.
    This scheduler don't use the automatic feature of the automatic allocation, then it isn't

    """
    name = 'HCP3'
        
    class SolverState(Enum):
        OUTSIDE_SEARCH = None  # Before search, after search.
        IN_ROOT_NODE = 1  # Executing the root node.
        IN_SEARCH = 2  # Executing the search code.
        AT_SOLUTION = 3  # After successful NextSolution and before EndSearch.
        NO_MORE_SOLUTIONS = 4  # After failed NextSolution and before EndSearch.
        PROBLEM_INFEASIBLE = 5  # After search, the model is infeasible.

    def __init__(self, allocator, resource_manager=None, seed=0, **kwargs):
        SchedulerBase.__init__(self, seed, None)
        self.manual_allocator = allocator
        
        # kwargs 
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
        if self.manual_allocator and not self.manual_allocator.resource_manager:
            self.manual_allocator.set_resource_manager(self.resource_manager)
        
        allocation = []
        
        resource_types = self.resource_manager.resource_types
        avl_resources = self.resource_manager.current_availability
        if self.manual_allocator:
            self.manual_allocator.set_resources(avl_resources)
                    
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
        for node in self.resource_manager.node_names:
            avl_res = avl_resources[node]
            for idx, job_obj in enumerate(es):
                job_id = job_obj.id
                
                if not(job_id in cons_qjobs):
                    current_qjobs.add(job_id)
                    cons_qjobs[job_id] = [False, 0, {}, None]
                    priorized_jobs.add((job_id, self._job_priority_slowdown(job_obj, cur_time)))
                        
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
                exp_duration = max(1, t[-1].expected_duration)
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
            schedule_plan = {}
            args = (schedule_plan, cur_time, cons_qjobs, remaining_priorized_jobs, es_dict, resource_types, avl_resources)
            kwargs = {'timelimit':timelimit, 'prev_sched':prev_sched}
            function = getattr(self, 'cp_model')
            function(*args, **kwargs)

            solver_state = schedule_plan.pop('solver_state')
            best_z = schedule_plan.pop('best_z')
            best_z_list.append(best_z)
            
            if solver_state == self.SolverState.PROBLEM_INFEASIBLE:
                break
            limit_reached = schedule_plan.pop('limit_reached')            
            
            disp_jobs = 0
            prev_sched = [] 
            for stime, job_id, _ in schedule_plan.values():
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
        
        allocation = self.allocate(cur_time, schedule_plan, es_dict, self.debug)
        # This is useful for print and also to create the unsuccessful data
        dispatched_jobs = 0
        queued_job_ids = []
        for a in allocation:
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
                 
        return allocation + list(decision_jobs.values()), []
    
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
        allocation = self.manual_allocator.allocate(to_schedule_now, cur_time, skip=True)  # , debug=debug)

        # All jobs to be scheduled later are considered as discarded by the allocator
        return allocation + [self.dispatching_tuple(ev.id) for ev in to_schedule_later]
    
    def cp_model(self, decision_jobs, cur_time, cons_qjobs, priorized_jobs, es_dict, resource_types, avl_resources, timelimit=15000, debug=False, prev_sched=[]):
        """
        Implementation of the CP Model using OR-Tools to generate the schedule plan.

        @param es: Queued jobs to be scheduled.
        @param es_dict: Dictionary of the current jobs.
        @param temp_sched: Storages the scheduled jobs.
        @param timelimit: Limit of the search process in ms.
        @param cur_time: Current simulated time
        @param avl_resources: Availability of the resources
        @param _debug: Debug flag.

        @return True is solved, False otherwise.

        """
        parameters = pywrapcp.Solver_DefaultSolverParameters()
        # parameters.trace_search = True
        solver = pywrapcp.Solver(self.name, parameters)
        running_jobs = self.resource_manager.current_allocations
        job_map = {}
        
        wc_makespan = 0
        #=======================================================================
        # Calculate first the remaining makespan of running jobs        
        #=======================================================================
        for job_id in running_jobs:
            e = es_dict[job_id]
            job_map[job_id] = e
            remaning_duration = max(1, e.expected_duration - (cur_time - e.start_time))
                
            wc_makespan += remaning_duration 
        
        #=======================================================================
        # Add the expected duration of queued jobs to the list of makespans
        #=======================================================================
        for job_id, prop in cons_qjobs.items():
            job_obj = prop[-1]
            job_map[job_id] = job_obj
            expected_duration = max(1, job_obj.expected_duration)
            
            wc_makespan += expected_duration 

        #=======================================================================
        # The minimum duration value must be 1, since ORTools doesnt accept 
        # duration eq to 0.
        #=======================================================================
        dict_vars = {}
        for job_id in list(running_jobs) + list(cons_qjobs.keys()):
            job_obj = job_map[job_id]
            exp_duration = max(1, job_obj.expected_duration)
            start_min = 0
            
            if job_obj.start_time:
                start_max = start_min
                elapsed_time = (cur_time - job_map[job_id].start_time)
            else:
                start_max = wc_makespan
                elapsed_time = 0
 
            estimated_remaining_time = max(1, (exp_duration - elapsed_time))

            var = solver.FixedDurationIntervalVar(start_min, start_max, estimated_remaining_time, False, job_id)
            dict_vars[job_id] = var
        
        for job_id in prev_sched:
            cst = dict_vars[job_id].StartExpr().Var() >= 0 
            solver.AddConstraint(cst)
                         
        _keys = self.resource_manager.resource_types
        total_capacity = self.resource_manager.system_capacity('total')
        total_demand = {}
        for _key in _keys:
            # total_capacity[_key] = self.resource_manager.#sum([ resources[_key] for node, resources in avl_resources.items()])
            # The resources used by running jobs are loaded into the capacity
            # total_capacity[_key] += sum(es_dict[job_id].requested_nodes * es_dict[job_id].requested_resources[_key] for job_id in running_jobs)
            job_ids = list(dict_vars.keys())
            total_demand[_key] = [ es_dict[job_id].requested_nodes * es_dict[job_id].requested_resources[_key] for job_id in job_ids]
            if sum(total_demand[_key]) == 0:
                continue
            _name = 'cum_{}'.format(_key)
            vars = [dict_vars[job_id] for job_id in job_ids]
            _cum = solver.Cumulative(vars, total_demand[_key], total_capacity[_key], _name)
            solver.AddConstraint(_cum)
            
        priorized_id_prb_vars = [job_id for job_id in running_jobs] + [ job_id for job_id in priorized_jobs]
        
        slowdowns = []
        all_intervals = []
        
        digits = max(self.significant, len(str(wc_makespan)))
        
        for job_id in priorized_id_prb_vars:
            job_obj = es_dict[job_id]
            job_model = dict_vars[job_id]
            # Queued jobs
            if not job_obj.start_time:
                expected_duration = max(1, job_obj.expected_duration)                
                job_start = job_model.SafeStartExpr(wc_makespan - expected_duration)                
                current_waiting_time = (cur_time - job_obj.queued_time)                
                wt_job = job_start + current_waiting_time
                
                if job_obj.expected_duration == 0:
                    slowdown = solver.Max([solver.IntConst(1), wt_job]) * (10 ** digits)
                else:
                    slowdown = ((wt_job + expected_duration) * (10 ** digits)) // expected_duration 
            # Running jobs
            else:
                slowdown = solver.IntConst(0)
            slowdowns.append(slowdown)
            
            all_intervals.append(job_model)

        monitors = []

        # Minimize the slowdown
        objective_var = solver.Sum(slowdowns).Var()
        objective_monitor = solver.Minimize(objective_var, 1)
        monitors.append(objective_monitor)
        
        db = solver.HeuristicSearch(all_intervals)

        # Utilities
        limit_monitor = solver.TimeLimit(timelimit)
        monitors.append(limit_monitor)
        
        best_monitor = solver.BestValueSolutionCollector(False)
        best_monitor.AddObjective(objective_var)
        best_monitor.Add(all_intervals)
        monitors.append(best_monitor)

        solved = solver.Solve(db, monitors)
        
        if solved == True:
            for job_id in priorized_jobs:
                job_model = dict_vars[job_id]
                if best_monitor.Solution(0).StartValue(job_model) == 0:
                    decision_jobs[job_id] = self.dispatching_tuple(job_id, cur_time)
                else:
                    decision_jobs[job_id] = self.dispatching_tuple(job_id)
            decision_jobs['best_z'] = best_monitor.ObjectiveValue(0)
        exec_time = solver.WallTime()
        decision_jobs['solver_state'] = 4 if exec_time < timelimit and solved else solver.State()
        solver.EndSearch()
        
        solver = None        
        decision_jobs['limit_reached'] = exec_time >= timelimit  # Should be == but it could take an extra ms to stop the solver. 
        if not solved:
            for job_id in cons_qjobs:
                decision_jobs[job_id] = self.dispatching_tuple(job_id)

        return solved
    
    def get_id(self):
        """
        Returns the full ID of the scheduler, including policy and allocator.

        :return: the scheduler's id.
        """
        allocator_id = self.manual_allocator.get_id() if self.manual_allocator else '' 
        return '-'.join([self.name, allocator_id])

    def _job_priority_slowdown(self, job_obj, cur_time):
        exp_duration = job_obj.expected_duration
        wait_time = cur_time - job_obj.queued_time
        if exp_duration == 0:
            job_slowdown = max(1, wait_time)
        else:  
            job_slowdown = (exp_duration + wait_time) / exp_duration
        return (-job_slowdown
            ,
        job_obj.expected_duration * sum([job_obj.requested_nodes * val for attr, val in job_obj.requested_resources.items()])
        )
            
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
