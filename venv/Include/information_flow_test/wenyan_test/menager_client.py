import manager

# 进程间共享变量
manager_client = manager.ManagerClient(manager.MANAGER_DOMAIN, manager.MANAGER_PORT, manager.MANAGER_AUTH_KEY)
share_dict = manager_client.get_dict()
open_qq_login_lock = manager_client.get_open_qq_login_lock()