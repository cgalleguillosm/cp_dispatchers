====================
CP-based Dispatchers
====================

The dispatchers presented below correspond to the indicated publications. All of those dispatchers use `OR-Tools <https://developers.google.com/optimization/>`_ to model and then solve the dispatching problem, and were implemented to work with the 
`AccaSim <https://accasim.readthedocs.io/en/latest/>`_ simulator. 

**Dispatchers**

	* A Job Dispatcher for Large and Heterogeneous HPC Systems Running Modern Applications `link <#>`_
		* `PCP21 <PCP21/pcp21_dispatcher.py>`_ (To use the dispatcher PCP21, the kwarg **sched_bt** must be equal to False, instead to use PCP21 sched_bt=True)
		* `HCP'19 <HCP/hcp3_scheduler.py>`_ (idem as HCP_3 from Constraint Programming-based Job Dispatching for Modern HPC Applications)
		* `PCP'19 <PCP/pcp3_scheduler.py>`_ (idem as PCP_3 from Constraint Programming-based Job Dispatching for Modern HPC Applications)


	*  Constraint Programming-based Job Dispatching for Modern HPC Applications `link <https://link.springer.com/chapter/10.1007/978-3-030-30048-7_26>`_

		** **Hybrid CP-based dispatchers**
			* `HCP <HCP/hcp_scheduler.py>`_ 
			* `HCP_1 <HCP/hcp1_scheduler.py>`_  
			* `HCP_2 <HCP/hcp2_scheduler.py>`_
			* `HCP_3 <HCP/hcp3_scheduler.py>`_   
				
		** **Pure CP-based dispatchers**
			* `PCP <PCP/pcp_scheduler.py>`_ 
			* `PCP_1 <PCP/pcp1_scheduler.py>`_  
			* `PCP_2 <PCP/pcp2_scheduler.py>`_
			* `PCP_3 <PCP/pcp3_scheduler.py>`_