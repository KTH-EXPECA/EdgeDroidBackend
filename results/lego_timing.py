#!/usr/bin/env python3
"""
 Copyright 2019 Manuel Olguín
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""


import json
import struct
from typing import Dict

from scapy.all import *


class LEGOTCPdumpParser():
    pkts = []

    def __init__(self, pcapf):
        self.pkts = rdpcap(pcapf)

    def extract_incoming_timestamps(self, dport: int) -> Dict[int,
                                                              list]:
        # pkts = rdpcap(pcapf)
        processed_frames = dict()
        pkts = [pkt for pkt in self.pkts if
                pkt[TCP].dport == dport and Raw in pkt]

        for pkt in pkts:
            data = bytes(pkt[TCP].payload)
            h_len_net = data[:4]
            data = data[4:]
            try:
                (h_len,) = struct.unpack('>I', h_len_net)
                header_net = data[:h_len]
                (header,) = struct.unpack('>{}s'.format(h_len), header_net)
                d_header = json.loads(header.decode('utf-8'))

                # store all timestamps
                if d_header['frame_id'] not in processed_frames.keys():
                    processed_frames[d_header['frame_id']] = []

                processed_frames[d_header['frame_id']].append(
                    pkt.time * 1000)


            except Exception as e:
                # print(e)
                continue

        return processed_frames

    def extract_outgoing_timestamps(self, sport: int) -> Dict[int, list]:
        # pkts = rdpcap(pcapf)
        processed_frames = dict()
        packets = [pkt for pkt in self.pkts if pkt[TCP].sport == sport and
                   Raw in pkt]

        for i in range(len(packets)):
            pkt = packets[i]
            data = bytes(pkt[TCP].payload)
            h_len_net = data[:4]
            data = data[4:]
            try:
                (h_len,) = struct.unpack('>I', h_len_net)
                while len(data) < h_len:
                    i += 1
                    data += bytes(packets[i][TCP].payload)

                header_net = data[:h_len]
                (header,) = struct.unpack('>{}s'.format(h_len), header_net)
                d_header = json.loads(header.decode('utf-8'))

                # store all timestamps
                if d_header['frame_id'] not in processed_frames.keys():
                    processed_frames[d_header['frame_id']] = []

                processed_frames[d_header['frame_id']].append(pkt.time * 1000)


            except Exception as e:
                # print(e)
                continue

        return processed_frames


if __name__ == '__main__':
    parser = LEGOTCPdumpParser('tcp.pcap')
    print(parser.extract_incoming_timestamps(8098))
    print(parser.extract_outgoing_timestamps(8111))
