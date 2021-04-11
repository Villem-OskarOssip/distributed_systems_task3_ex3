import sys
import random
from datetime import datetime, timedelta

filename = sys.argv[1]
processes = {}
program_running = True
g_time = datetime.now()


def get_coordinator_id():
    for k, v in processes.items():
        if v['coordinator']:
            return k
    return None


def init_coordinator():
    print('Program started.')
    print('To exit the program type exit.')
    print('Choosing a random coordinator.')
    choice = random.choice(list(processes.items()))
    pid = int(choice[0])
    choice[1]['coordinator'] = True
    processes[pid] = choice[1]
    print('Randomly chosen coordinator:')
    print_process_with_time(pid)


def process_name(name):
    split = name.split('_')
    name = split[0]
    val = int(split[1].strip())
    return name, val


def update_names():
    print('Updating process names...')
    for _, v in processes.items():
        name, val = process_name(v['name'])
        val += 1
        v['name'] = name + '_' + str(val)
    get_list()


def is_highest_pid(pid):
    candidates = []
    for k, v in processes.items():
        if not v['frozen']:
            candidates.append(k)
    return pid == max(candidates)


def get_greater_pids(pid):
    pids = []
    for k, v in processes.items():
        if k > pid and not v['frozen']:
            pids.append(k)
    return pids


def higher_pids_election(pids, current_coord_id):
    lowest_pid = min(pids)
    others = [pid for pid in pids if pid != lowest_pid]
    if len(others) == 0:
        print('Election has finished.')
        if current_coord_id != lowest_pid:
            update_coordinator(current_coord_id, lowest_pid)
            update_times(lowest_pid)
        update_names()
        return
    print('PID: {0} sent an election message to PIDs: {1}'.format(lowest_pid, others))

    while len(others) > 0:
        higher_pids_election(others, current_coord_id)
        return


def update_coordinator(old_coord_pid, new_coord_pid):
    processes[old_coord_pid]['coordinator'] = False
    processes[new_coord_pid]['coordinator'] = True
    print('New coordinator:')
    print_process(new_coord_pid)


def update_times(coord_pid):
    global g_time
    print("Updating process clocks...")
    g_time = processes[coord_pid]['time']


def get_random_pid():
    candidates = []
    for k, v in processes.items():
        if not v['frozen']:
            candidates.append((k, v))
    choice = random.choice(candidates)
    return int(choice[0])


def start_election():
    print('Starting election...')
    current_coord_id = get_coordinator_id()
    if current_coord_id is None:
        print('No current coordinator.')
        current_coord_id = get_random_pid()
        print('Starting new election from random PID:', current_coord_id)
    else:
        print('Current coordinator:')
        print_process_with_time(current_coord_id)

    print('Sending election messages to all processes with higher PIDs.')
    if is_highest_pid(current_coord_id):
        processes[current_coord_id]['coordinator'] = True
        print('No OK messages received.')
        print('Current coordinator has the highest PID, sending Coordinator messages to all processes with lower IDs.')
        others = [x for x in processes.keys() if x < current_coord_id]
        print('Processes with lower PIDs: ' + str(others))
        update_times(current_coord_id)
    else:
        processes_with_higher_pids = get_greater_pids(current_coord_id)
        print('Received OK messages from processes with higher PIDs: {0}'.format(processes_with_higher_pids))
        print('Election starting between them.')
        higher_pids_election(processes_with_higher_pids, current_coord_id)
        global start
        start = datetime.now()


def init_read_file():
    with open(filename, 'r') as f:
        lines = f.readlines()
        set_processes(lines)
        f.close()


def str_to_datetime(time_string):
    return datetime.strptime(time_string, '%H:%M')


def set_processes(data):
    for process in data:
        split = process.strip('\n').split(',')
        pid = int(split[0])
        name = split[1].strip()
        time_string = split[2].strip().replace("pm", "").replace("am", "")

        time = str_to_datetime(time_string)

        processes[pid] = {
            'name': name,
            'coordinator': False,
            'time': time,
            'frozen': False
        }


def kill(user_input):
    target = int(user_input.split(' ')[1])
    print('Killing process with PID:', target)
    print_process(target)
    is_coord = processes[target]['coordinator']
    del processes[target]
    if is_coord:
        print('Killed coordinator, starting a new election.')
        # start = time.clock()
        global start
        start = datetime.now()
        start_election()
        # time_elapsed = time.clock() - start
        # print('Election time elapsed:', str(time_elapsed))


def freez(user_input):
    target = int(user_input.split(' ')[1])
    print('Freezing process with PID:', target)
    print_process(target)
    proccess = processes[target]
    proccess['frozen'] = True
    if proccess['coordinator']:
        print('Froze coordinator, starting a new election.')
        processes[target]['coordinator'] = False
        start_election()


def unfreeze(user_input):
    target = int(user_input.split(' ')[1])
    print('Unfreezing process with PID:', target)
    print_process(target)
    proccess = processes[target]
    proccess['frozen'] = False
    if is_highest_pid(target):
        print('Froze process has highest ID. Starting a new election.')
        start_election()


def update_processes(lines):
    for line in lines:
        split = line.split(',')
        pid = int(split[0])
        if pid not in processes:
            name = split[1].strip()
            time_string = split[2].strip().replace("pm", "").replace("am", "")
            time = str_to_datetime(time_string)
            processes[pid] = {
                'name': name,
                'coordinator': False,
                'time': time,
                'frozen': False
            }
            print('Reload process:')
            print_process(pid)


def reload():
    with open(filename, 'r') as f:
        lines = f.readlines()
        update_processes(lines)
        # start = time.clock()
        start_election()
        # time_elapsed = time.clock() - start
        # print('Time elapsed for election:', time_elapsed)
        f.close()


def print_process(pid):
    item = processes[pid]
    if item['coordinator'] and item['frozen']:
        print(f"{pid}, {item['name']} (Coordinator) (Frozen)")
    elif item['coordinator']:
        print(f"{pid}, {item['name']} (Coordinator)")
    elif item['frozen']:
        print(f"{pid}, {item['name']} (Frozen)")
    else:
        print(f"{pid}, {item['name']}")


def print_process_with_time(pid):
    item = processes[pid]
    if item['coordinator']:
        print(f"{pid}, {item['name']}, {datetime_to_str(g_time)}, (Coordinator)")
    else:
        print(f"{pid}, {item['name']}, {datetime_to_str(g_time)}")


def get_list():
    for k, v in processes.items():
        print_process(k)


def clock():
    for k, v in processes.items():
        time = time_elapsed()
        print(f"{v['name']}, {datetime_to_str(time)}")


def datetime_to_str(time):
    hours = str(time.hour)
    minutes = str(time.minute)
    if len(hours) == 1:
        hours = "0" + hours
    if len(minutes) == 1:
        minutes = "0" + minutes
    return hours + ":" + minutes


def set_time(user_input):
    target = int(user_input.split(' ')[1])
    new_time = str_to_datetime(user_input.split(' ')[2])
    print('Changing time for process with PID:', target)

    process = processes.get(target)
    print(f"PID: {target}. Old time: {datetime_to_str(process['time'])}. New time: {datetime_to_str(new_time)}")
    process['time'] = new_time
    print_process_with_time(target)
    is_coord = processes[target]['coordinator']
    if is_coord:
        print('Changed coordinator time, synchronizing all processes...')
        global g_time, start
        g_time = new_time
        print('Resetting elapsed time...')
        start = datetime.now()


def get_time_change():
    now = datetime.now()
    dif = now - start

    elapsed_hours = dif.total_seconds() / 60 / 60
    elapsed_minutes = dif.total_seconds() / 60

    time_change = timedelta(hours=elapsed_hours, minutes=elapsed_minutes)
    return time_change


def time_elapsed():
    return g_time + get_time_change()


def print_elapsed_time():
    print(f"Elapsed time: {get_time_change()}")


init_read_file()
init_coordinator()

start = datetime.now()
start_election()

while program_running:
    print_elapsed_time()
    print("===================================")
    user_input = input().lower()
    if user_input == 'list':
        get_list()
    elif 'kill' in user_input:
        kill(user_input)
    elif user_input == 'reload':
        reload()
    elif user_input == 'clock':
        clock()
    elif 'set-time' in user_input:
        set_time(user_input)
    elif 'unfreeze' in user_input:
        unfreeze(user_input)
    elif 'freeze' in user_input:
        freez(user_input)
    elif user_input == 'exit':
        program_running = False
    user_input = ""
