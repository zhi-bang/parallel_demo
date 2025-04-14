import multiprocessing
import time
import os

""" Implementing Combined Data and Pipeline Parallelism Using Parent-Child Processes """
 
# 流水段1：接收外部输入数据并传给 task2
def task1(input_data, output_queue):
    print(f"流水段1 PID={os.getpid()}")
    time.sleep(1)
    data = input_data + " -> 经task1处理"
    print(f"流水段1 完成，输出：{data}")
    output_queue.put(data)

# 流水段2：读取 task1 的输出，处理后传给 task3
def task2(input_queue, output_queue):
    print(f"流水段2 PID={os.getpid()}")
    data = input_queue.get()
    data += " -> 经task2处理"
    time.sleep(1)
    print(f"流水段2 完成，输出：{data}")
    output_queue.put(data)

# 流水段3：读取 task2 的输出，处理后传给 task4
def task3(input_queue, output_queue):
    print(f"流水段3 PID={os.getpid()}")
    data = input_queue.get()
    data += " -> 经task3处理"
    time.sleep(1)
    print(f"流水段3 完成，输出：{data}")
    output_queue.put(data)

# 流水段4：读取最终结果，打印
def task4(input_queue):
    print(f"流水段4 PID={os.getpid()}")
    data = input_queue.get()
    data += " -> 经task4处理"
    time.sleep(1)
    print(f"流水段4 完成，最终结果：{data}")

# 主控进程要执行的逻辑：启动四个子任务
def master_process(name):
    print(f"主控进程 {name} 启动，PID={os.getpid()}")

    # 定义三个队列用于通信
    q1 = multiprocessing.Queue()  # task1 → task2
    q2 = multiprocessing.Queue()  # task2 → task3
    q3 = multiprocessing.Queue()  # task3 → task4

    # 初始输入数据（传给 task1）
    init_data = "初始数据 from 主程序"

    # 创建各流水段进程
    p1 = multiprocessing.Process(target=task1, args=(init_data, q1))
    p2 = multiprocessing.Process(target=task2, args=(q1, q2))
    p3 = multiprocessing.Process(target=task3, args=(q2, q3))
    p4 = multiprocessing.Process(target=task4, args=(q3,))
    
    # 启动进程（下游先启动）
    p4.start()
    p3.start()
    p2.start()
    p1.start()

    # 等待所有进程结束
    p1.join()
    p2.join()
    p3.join()
    p4.join()

    print(f"主控进程 {name} 的所有子任务完成")

# 主程序：创建两个主控进程
if __name__ == "__main__":
    multiprocessing.set_start_method('fork')  # Windows 可注释
    
    master1 = multiprocessing.Process(target=master_process, args=("数据并行A",))
    master2 = multiprocessing.Process(target=master_process, args=("数据并行B",))
    master3 = multiprocessing.Process(target=master_process, args=("数据并行C",))
    master4 = multiprocessing.Process(target=master_process, args=("数据并行D",))
    master5 = multiprocessing.Process(target=master_process, args=("数据并行E",))
    master6 = multiprocessing.Process(target=master_process, args=("数据并行F",))
    s = time.time()
    master1.start()
    master2.start()
    master3.start()
    master4.start()
    master5.start()
    master6.start()

    master1.join()
    master2.join()
    master3.join()
    master4.join()
    master5.join()
    master6.join()
    e = time.time()
    print(f"用时{e-s}s")

    print("所有主控进程与子进程执行完毕")
