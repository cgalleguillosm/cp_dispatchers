from re import escape, match
from sortedcontainers import SortedListWithKey

class DataDrivenPredictor:
    
    USERNAME = 'user_id'
    JOB_NAME = 'job_name'
    QUEUE = 'queue'
    SUBMISSION_TIME = 'queued_time'
    START_TIME = 'start_time'
    END_TIME = 'end_time'
    DURATION = 'duration'
    REQUESTED_WALLTIME = 'requested_time'

    """
     The similar profile is identified using a set of consecutive rules. First, a 
     full profile match is searched for, then if this does not exist in the user 
     history, a profile where the job name has the same prefix is looked up. 
     This follows from the observation that users often name jobs with similar 
     durations with the same job name followed by a number (e.g. "job1","job2"). 
     If this is unsuccessful, we allow for resources used to differ, as long as 
     the full job name, # queue and wall-time are the same. If also this search 
     fails, we look for the same match but with the name prefix rather than the exact name. 
     If none of these rules give a match, we look for the last job with the same 
     name, or, as a last resort, the same name prefix. If all rules fail, then 
     we take the wall-time as the predicted duration. In all cases, the prediction
     is capped by the wall-time.
    """
  
    def __init__(self, profile_attrs, max_wtimes, actual_duration_calc, resource_req_attrs, attrs_names=None, similarity=True, app_number=True):
        """
        
        :param profile_attrs: Atributes to estimate the duration. It does not include the username, just the relevant jobs attributes. 
            Queued, started and ended time should not be included.
        
        """
        Predictor.__init__(self)
        self.profile_attrs = profile_attrs
        self.max_wtimes = max_wtimes
        self.actual_duration_calc = actual_duration_calc
        self.resource_req_attrs = resource_req_attrs
        self.similarity = similarity
        
        if attrs_names:
            for k, v in attrs_names.items():
                setattr(self, k, v)
                
        MINIMAL_ATTRIBUTES = [self.USERNAME, self.JOB_NAME, self.QUEUE, self.SUBMISSION_TIME, self.START_TIME, self.END_TIME, self.DURATION]
        self.knowledge_base = set(MINIMAL_ATTRIBUTES + profile_attrs)
        self.full_profiles = {}  # SortedDict()
        
    def predict_duration(self, current_job):
        user = getattr(current_job, self.USERNAME)
        current_job_name = getattr(current_job, self.JOB_NAME)
        actual_duration = self.actual_duration_calc(current_job)
        requested_walltime = self.walltime_func(getattr(current_job, self.REQUESTED_WALLTIME))
        
        h_data = self._hash_function([getattr(current_job, p) for p in self.profile_attrs])          
        
        user_profiles = self.full_profiles.get(user, None) 
        if user_profiles:                        
            # Rule 1 full matching
            full_match = self.full_profiles[user].get(h_data, None)
            if full_match:
                return actual_duration, min(full_match['duration'], requested_walltime), False
    
            full_matched_names = SortedListWithKey(key=lambda x:(-x[1]))
            partial_matched_names = SortedListWithKey(key=lambda x:(-x[1], -x[2]))
                        
            for id, profile_data in user_profiles.items():
                profile_job_name = profile_data[self.JOB_NAME]
                if profile_job_name == current_job_name:
                    full_matched_names.add((id, profile_data[self.END_TIME], profile_data[self.DURATION]))
                    partial_matched_names.add((id, len(current_job_name), profile_data[self.END_TIME], profile_data[self.DURATION]))
                else:
                    match, name_similarity = self._partial_match(profile_job_name, current_job_name)
                    if match:
                        partial_matched_names.add((id, name_similarity, profile_data[self.END_TIME], profile_data[self.DURATION]))
                                    
            # Rule 2 similar job name and remaining job features are maintained.
            if partial_matched_names:
                partial_profile = list(self.profile_attrs)
                partial_profile.remove(self.JOB_NAME)
    
                for id, name_similarity, _, _ in partial_matched_names:
                    profile = user_profiles[id]
                    prev_duration = self._match_profile(profile, current_job, partial_profile)
                    if prev_duration:
                        return actual_duration, min(prev_duration, requested_walltime), False
    
            # Rule 3 Exact job name, variable resource request and remaining job features are maintained.
            if full_matched_names:
                partial_profile = list(self.profile_attrs)
                for e in self.resource_req_attrs:
                    partial_profile.remove(e)
                if self.similarity:
                    valid_profiles = SortedListWithKey(key=lambda x:(-x[0], -x[1],))
                
                for id, name_similarity, _ in full_matched_names:
                    profile = user_profiles[id]
                    prev_duration = self._match_profile(profile, current_job, partial_profile)
                    if prev_duration:
                        if not self.similarity:
                            return actual_duration, min(prev_duration, requested_walltime), False
                        else:
                            resource_sim = self._count_similarity(profile, current_job, self.resource_req_attrs)
                            valid_profiles.add((name_similarity, resource_sim, prev_duration))
                        
                if self.similarity and valid_profiles:
                    return actual_duration, min(valid_profiles[0][-1], requested_walltime), False                

            # Rule 4 Partial job name and variable resource request
            if partial_matched_names:
                partial_profile = list(self.profile_attrs)
                for e in [self.JOB_NAME] + self.resource_req_attrs:
                    partial_profile.remove(e)
                
                if self.similarity:
                    valid_profiles = SortedListWithKey(key=lambda x:(-x[0], -x[1],))
                
                for id, name_similarity, _, _ in partial_matched_names:
                    profile = user_profiles[id]
                    prev_duration = self._match_profile(profile, current_job, partial_profile)
                    if prev_duration:
                        if not self.similarity:
                            return actual_duration, min(prev_duration, requested_walltime), False
                        else:
                            resource_sim = self._count_similarity(profile, current_job, self.resource_req_attrs)
                            valid_profiles.add((name_similarity, resource_sim, prev_duration))
                if self.similarity and valid_profiles:
                    return actual_duration, min(valid_profiles[0][-1], requested_walltime), False
            
            # Rule 5 Exact job name
            if full_matched_names:
                _, _, prev_duration = full_matched_names[0]
                return actual_duration, min(prev_duration, requested_walltime), False
                    
            # Rule 6 partial job name
            if partial_matched_names:
                _, _, _, prev_duration = partial_matched_names[0]
                return actual_duration, min(prev_duration, requested_walltime), False
                
        return actual_duration, requested_walltime, True
    
    def add_kb(self, job, extra_attrs=[]):
        user = getattr(job, self.USERNAME)
        if not(user in self.full_profiles):
            self.full_profiles[user] = {}
        h_data = self._hash_function([getattr(job, p) for p in self.profile_attrs])
        job_dict = {k: getattr(job, k) for k in self.knowledge_base}
        self.full_profiles[user][h_data] = job_dict
        
    def _get_max_walltime(self, queue):
        return self.max_wtimes[queue]
    
    def _hash_function(self, values):
        return ''.join([str(v) for v in values])

    def _match_profile(self, profile, data, profile_attrs):
        for f in profile_attrs:
            if profile[f] != getattr(data, f):
                return None
        return profile[self.DURATION]
    
    def _count_similarity(self, profile, data, profile_attrs):
        return sum([1 if profile[f] == getattr(data, f) else 0 for f in profile_attrs])
    
    def _partial_match(self, stored_cmd, cmd):
        found = False
        for i, (s, t) in enumerate(zip(stored_cmd, cmd)):
            if s != t:
                return found, i
            else:
                found = True
        return found, len(stored_cmd)
    
    def walltime_func(self, original_wt):
        return original_wt