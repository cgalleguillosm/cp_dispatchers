====================
CP-based Dispatchers
====================

The dispatchers presented below correspond to the ones used in the paper entitled "Constraint Programming-based Job Dispatching for Modern HPC Applications". All of those dispatchers
use `OR-Tools <https://developers.google.com/optimization/>`_ to model and then solve the dispatching problem, and were implemented to work with the 
`AccaSim <https://accasim.readthedocs.io/en/latest/>`_ simulator. 

**Dispatchers**
	
	* **Hybrid CP-based dispatchers**
		* `HCP <HCP/hcp_scheduler.py>`_ 
		* `HCP\ :sub:`1`\ <HCP/hcp1_scheduler.py>`_  
		* `HCP\ :sub:`2`\ <HCP/hcp2_scheduler.py>`_
		* `HCP\ :sub:`3`\ <HCP/hcp3_scheduler.py>`_   
			
	* **Pure CP-based dispatchers**
		* [PCP/pcp1_scheduler.py] (PCP)
		* [PCP/pcp1_scheduler.py] (PCP\ :sub:`1`\)
		* [PCP/pcp2_scheduler.py] (PCP\ :sub:`2`\)
		* [PCP/pcp3_scheduler.py] (PCP\ :sub:`3`\)
