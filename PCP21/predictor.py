import time
from os import path

from accasim.base.additional_data import AdditionalData


class SWFLastNPredictorInterface(AdditionalData):
    
    USERNAME = 'user_id'
    JOB_NAME = 'job_name'
    QUEUE = 'queue'
    SUBMISSION_TIME = 'queued_time'
    START_TIME = 'start_time'
    END_TIME = 'end_time'
    DURATION = 'duration'
    REQUESTED_WALLTIME = 'expected_duration'
    
    def __init__(self, event_manager, n=2, attrs_names=None, debug=False, **kwargs):
        AdditionalData.__init__(self, event_manager)
        self.est_path = path.join(self.constant.RESULTS_FOLDER_PATH, 'estimator_' + self.constant.WORKLOAD_FILENAME)        
        self.user_history = {}
        self.last_n = n
        
        if attrs_names:
            for k, v in attrs_names.items():
                setattr(self, k, v)
        
        self.req_w_diff = 0
        self.pred_diff = 0
        self.stored_data = []
        self.spent_time = 0
        
        self.create_file()
        
    def exec_after_completion(self, removed_jobs):
        for job_obj in removed_jobs:
            self.add_kb(job_obj)
    
    def exec_after_dispatching(self, job_dict, to_dispatch, rejected):
        pass
    
    def exec_after_submission(self, **kwargs):
        pass
    
    def exec_before_completion(self, **kwargs):
        pass
    
    def exec_before_dispatching(self, job_dict, queued_job_ids):
        for job_id in queued_job_ids:
            job_obj = job_dict[job_id]
            if hasattr(job_obj, '_user_walltime'):
                continue
            init_time = time.time()
            _, predicted, req_walltime = self.predict_duration(job_obj)
            ttime = (time.time() - init_time)
            self.spent_time += ttime 
            # print('job_id: {} - {} (Acum: {})'.format(job_id, ttime, self.spent_time))
            # job_obj.expected_duration = predicted
            # job_obj._user_walltime = req_walltime
            # if not hasattr(job_obj, '_user_walltime'):  # or (hasattr(job_obj, '_user_walltime') and (job_obj.expected_duration != predicted)):
            setattr(job_obj, 'expected_duration', predicted)
            setattr(job_obj, '_user_walltime', req_walltime)
            self.req_w_diff += abs(job_obj.duration - req_walltime)
            self.pred_diff += abs(job_obj.duration - predicted)
            self.push_data((job_id, job_obj.duration, req_walltime, predicted,))
    
    def exec_before_submission(self, es):
        pass
        
    def push_data(self, data):
        self.stored_data.append(data)
        if len(self.stored_data) > 5000:
            self.save_to_file()
    
    def create_file(self):
        with open(self.est_path, 'w') as f:
            f.write('User estimation difference: {}\n'.format(self.req_w_diff))
            f.write('Data-driven estimation difference: {}\n'.format(self.pred_diff))
            f.write('id;runtime;walltime;prediction\n')
    
    def save_to_file(self):
        with open(self.est_path, 'a') as f:
            for data in self.stored_data:
                f.write('{};{};{};{}\n'.format(*data))     
        self.stored_data = []

    def stop(self):
        self.save_to_file()
        print('Spent time: {}'.format(self.spent_time)) 
        
    def add_kb(self, job):
        user_id = getattr(job, self.USERNAME)
        job_duration = getattr(job, self.REQUESTED_WALLTIME)
          
        if not(user_id in self.user_history):
            self.user_history[user_id] = (job_duration, [job_duration],)
            return
        
        _, previous_durations = self.user_history[user_id]
        size = len(previous_durations)
        if size == self.last_n:
            previous_durations.pop(0)
            size -= 1
        previous_durations.append(job_duration)
        size += 1
        self.user_history[user_id] = (self._calculate_average(previous_durations, size), previous_durations,)        
    
    def predict_duration(self, current_job):
        user = getattr(current_job, self.USERNAME)
        walltime = getattr(current_job, self.REQUESTED_WALLTIME)
        real_duration = getattr(current_job, self.DURATION)
        
        data = self.user_history.get(user, None)
        if data:
            return (real_duration, data[0], walltime)
        return (real_duration, walltime, walltime)
        
    def _calculate_average(self, data, size):
        if size == 2:
            return (data[0] + data[1]) // 2
        return sum(data) // size

    def save_matching_debug(self, fout):
        pass
