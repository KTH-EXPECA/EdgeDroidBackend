from results.lego_timing import LEGOTCPdumpParser
import json


def load_results(client_idx):
    filename = '{:02}_stats.json'.format(client_idx)
    with open(filename, 'r') as f:
        return json.load(f, encoding='utf-8')


if __name__ == '__main__':
    data = load_results(0)
    video_port = data['ports']['video']
    result_port = data['ports']['result']

    parser = LEGOTCPdumpParser('tcp.pcap')

    server_in = parser.extract_incoming_timestamps(video_port)
    server_out = parser.extract_outgoing_timestamps(result_port)

    print(server_in)
    print(server_out)

    # TODO: build stats for each frame:
    # TODO: client send, server recv, server send, client recv

    for run_idx, run in enumerate(data['runs']):
        for frame in run['frames']:
            id = frame['frame_id']
            client_send = frame['sent']
            server_recv = server_in[id].pop(0)
            server_send = server_out[id].pop(0)
            client_recv = frame['recv']

            uplink = server_recv - client_send
            processing = server_send - server_recv
            downlink = client_recv - server_send

            rtt = uplink + processing + downlink

            print(
                '''
frame {}
client send {}
server recv {}
server send {}
client recv {}
uplink      {}
downlink    {}
processing  {}
total rtt   {}
                '''.format(
                    id, client_send, server_recv,
                    server_send, client_recv, uplink,
                    downlink, processing, rtt
                )
            )
