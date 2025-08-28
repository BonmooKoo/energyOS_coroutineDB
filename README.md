# energyOS_coroutineDB


Wait_list : new coroutine or migrated Coroutine
protected by lock

Work_queue : Ready Queue 
No lock only master coroutine can handle.

coroutine life cycle : 
new/migrated/woekn -> wait list -> schedule_one -> work queue
					|->> pop& resume
						|->> yield/undone -> work queue
						|->> finished -> kill coroutine



