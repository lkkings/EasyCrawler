from easycrawler.master import Master

Master(ip, client_port, worker_port, max_workers, max_clients).run()
print(ip, client_port, worker_port)