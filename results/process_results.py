import json
import operator
from collections import namedtuple

import pandas as pd
import psutil
from scapy.all import *

from results.lego_timing import LEGOTCPdumpParser

Frame = namedtuple('Frame', ['id', 'rtt', 'uplink',
                             'downlink', 'processing',
                             'client_send', 'client_recv',
                             'server_send', 'server_recv'])


def load_results(client_idx):
    filename = '{:02}_stats.json'.format(client_idx)
    with open(filename, 'r') as f:
        return json.load(f, encoding='utf-8')


def parse_client_stats(client_idx):
    data = load_results(client_idx)
    video_port = data['ports']['video']
    result_port = data['ports']['result']

    parser = LEGOTCPdumpParser('tcp.pcap')

    server_in = parser.extract_incoming_timestamps(video_port)
    server_out = parser.extract_outgoing_timestamps(result_port)

    for run_idx, run in enumerate(data['runs']):
        run_frames = []
        for frame in run['frames']:
            frame_id = frame['frame_id']
            client_send = frame['sent']
            server_recv = server_in[frame_id].pop(0)
            server_send = server_out[frame_id].pop(0)
            client_recv = frame['recv']

            uplink = server_recv - client_send
            processing = server_send - server_recv
            downlink = client_recv - server_send
            rtt = client_recv - client_send

            try:
                assert processing > 0
                run_frames.append(Frame(frame_id, rtt, uplink,
                                        downlink, processing,
                                        client_send, client_recv,
                                        server_send, server_recv))
            except AssertionError as e:
                print('Recv', server_recv)
                print('Send', server_send)
                print('Proc', processing)
                print('Run', run_idx)
                print('Frame {} of {}', frame_id, len(run['frames']))
                run_frames.append(Frame(frame_id, rtt, None, None, None,
                                        client_send, client_recv,
                                        server_send, server_recv))

        run_frames.sort(key=operator.attrgetter('id'))
        data['runs'][run_idx]['frames'] = run_frames

    return data


def plot_rtts(data):
    frames = [run['frames'] for run in data['runs']]
    fig, [ax1, ax2] = plt.subplots(1, 2, sharey=True)
    x_lim = 0
    for i, run in enumerate(frames):
        rtts = [frame.rtt for frame in run]
        proc = [frame.processing for frame in run]

        idx = [frame.id for frame in run]
        ax1.scatter(idx, rtts, label='Run {}'.format(i + 1), s=1)
        ax2.scatter(idx, proc, label='Run {}'.format(i + 1), s=1)

        x_lim = max(x_lim, len(idx))

    # y_lim = max(ax1.get_ylim()[-1], ax2.get_ylim()[-1])

    ax1.set_xlim(-50, x_lim + 50)
    ax2.set_xlim(-50, x_lim + 50)
    ax1.set_xlabel('Frame index')
    ax1.set_ylabel('Total RTT [ms]')
    ax2.set_xlabel('Frame index')
    ax2.set_ylabel('Processing time [ms]')
    ax1.set_yscale('log')
    ax2.set_yscale('log')

    plt.legend()
    plt.show()


def plot_avg_times(data):
    frames = [run['frames'] for run in data['runs']]
    fig, ax = plt.subplots()

    def autolabel(rects):
        """
        Attach a text label above each bar displaying its height
        """
        for rect in rects:
            height = rect.get_height()
            x_pos = rect.get_x() + rect.get_width() / 2.0
            y_pos = 0.5 * height + rect.get_y() if height >= 5.0 else \
                rect.get_y()
            ax.text(x_pos, y_pos,
                    '{:02.2f}'.format(height),
                    ha='center', va='bottom', weight='bold')

    results = []
    x = []
    for i, run in enumerate(frames):
        avg_up = 0
        count_up = 0
        avg_down = 0
        count_down = 0
        avg_proc = 0
        count_proc = 0
        for frame in run:
            up = frame.uplink
            down = frame.downlink

            if up and up > 0:
                avg_up += up
                count_up += 1

            if down and down > 0:
                avg_down += down
                count_down += 1

            if frame.processing:
                avg_proc += frame.processing
                count_proc += 1

        avg_up = avg_up / (1.0 * count_up)
        avg_down = avg_down / (1.0 * count_down)
        avg_proc = avg_proc / (1.0 * count_proc)

        results.append((avg_up, avg_down, avg_proc))
        x.append('Run {}'.format(i + 1))

    up = [y[0] for y in results]
    down = [y[1] for y in results]
    proc = [y[2] for y in results]

    rects1 = ax.bar(x, up, label='Avg uplink time')
    rects2 = ax.bar(x, proc, bottom=up, label='Avg processing time')
    rects3 = ax.bar(x, down,
                    bottom=[x + y for x, y in zip(up, proc)],
                    label='Avg downlink time')

    autolabel(rects1)
    autolabel(rects2)
    autolabel(rects3)

    ax.set_ylabel('Time [ms]')
    plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    plt.show()


def plot_task_times(data):
    fig, ax = plt.subplots()
    task_times = [
        run['end'] - run['init'] for run in data['runs']
    ]
    x = ['Run {}'.format(i + 1) for i in range(len(data['runs']))]
    ax.bar(x, task_times)
    ax.set_ylabel('Total task time [ms]')
    plt.show()


def plot_task_times_from_frames(data):
    task_times = []
    x = []
    for i, run in enumerate(data['runs']):
        send_times = [frame.client_send for frame in run['frames']]
        recv_times = [frame.client_recv for frame in run['frames']]

        task_times.append(max(recv_times) - min(send_times))
        x.append('Run {}'.format(i + 1))

    fig, ax = plt.subplots()
    task_times = [t / 1000.0 for t in task_times]
    ax.bar(x, task_times)
    ax.set_ylabel('Total task time [s]')
    plt.show()


def plot_cpu_load():
    df = pd.read_csv('system_stats.csv')
    start_time = df['timestamp'][0]
    df['timestamp'] = (df['timestamp'] - start_time) / (60 * 1000.0)
    df['avg_cpu_load'] = df['cpu_load'].rolling(11, center=True).mean()

    ax = df.plot(x='timestamp', y='cpu_load', alpha=0.5, label='CPU Load')
    df.plot(x='timestamp', y='avg_cpu_load',
            ax=ax, color='red', label='Avg. CPU Load')

    ax.set_xlabel('Time [m]')
    ax.set_ylabel('Load [%]')
    plt.show()


def plot_ram_usage():
    df = pd.read_csv('system_stats.csv')
    start_time = df['timestamp'][0]
    df['timestamp'] = (df['timestamp'] - start_time) / (60 * 1000.0)
    df['mem_avail'] = df['mem_avail'] / (1024.0 * 1024.0 * 1024)

    total_ram = psutil.virtual_memory().total / (1024.0 * 1024.0 * 1024)
    df['used_mem'] = total_ram - df['mem_avail']

    ax = df.plot(x='timestamp', y='used_mem', color='orange', label='Used RAM')
    ax.axhline(y=total_ram, color='red', label='Total RAM')
    # ax.set_yscale('log')
    ax.set_xlabel('Time [m]')
    ax.set_ylabel('Memory [GiB]')
    plt.legend()
    plt.show()


def split_tcpdump(client_idx, tcpdump):
    c_data = load_results(client_idx)

    ports = c_data['ports'].values()
    pkts = rdpcap(tcpdump)

    relevant_pkts = [
        pkt for pkt in pkts
        if pkt[TCP].sport in ports or pkt[TCP].dport in ports
    ]

    filename = '{:02}_dump.pcap'.format(client_idx)
    wrpcap(filename, relevant_pkts)


if __name__ == '__main__':
    data = parse_client_stats(0)
    plot_rtts(data)
    plot_avg_times(data)
    plot_task_times_from_frames(data)
    plot_ram_usage()
    plot_cpu_load()
