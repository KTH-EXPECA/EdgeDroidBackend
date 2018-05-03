from results.lego_timing import extract_incoming_timestamps, extract_outgoing_timestamps
import json

def load_results(client_idx):
    filename = '{:02}_stats.json'.format(client_idx)
    with open(filename, 'r') as f:
        return json.load(f, encoding='utf-8')

if __name__ == '__main__':
    data = load_results(0)
    video_port = data['ports']['video']
    result_port = data['ports']['result']

    server_in = extract_incoming_timestamps(video_port, 'tcp.pcap')
    server_out = extract_outgoing_timestamps(result_port, 'tcp.pcap')


    # TODO: build stats for each frame:
    # TODO: client send, server recv, server send, client recv
    print(server_in)
    print(server_out)


