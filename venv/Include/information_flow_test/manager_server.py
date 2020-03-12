import manager

def run():
    manager_server = manager.ManagerServer(manager.MANAGER_DOMAIN, manager.MANAGER_PORT, manager.MANAGER_AUTH_KEY)
    manager_server.run()
if __name__ == '__main__':
    run()