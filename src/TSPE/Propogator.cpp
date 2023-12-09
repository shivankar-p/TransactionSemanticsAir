/**
 * Copyright (c) 2020 University of Luxembourg. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are
 * permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list
 * of conditions and the following disclaimer in the documentation and/or other materials
 * provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors may be
 * used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF LUXEMBOURG AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE UNIVERSITY OF LUXEMBOURG OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

/*
 * EventFilter.cpp
 *
 *  Created on:
 *      Author: vinu.venugopal
 */

#include "Propogator.hpp"

#include <mpi.h>
//#include <__threading_support>
#include <cstring>
#include <iostream>
#include <iterator>
#include <list>
#include <string>
#include <vector>
#include <regex>
#include "../communication/Message.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

Propogator::Propogator(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
	D(cout << "EVENTFILTER [" << tag << "] CREATED @ " << rank << endl;)
	THROUGHPUT_LOG(
			datafile.open("Data/tp_log"+ to_string(rank) + ".tsv");
	)
}

Propogator::~Propogator() {
	D(cout << "EVENTFILTER [" << tag << "] DELETED @ " << rank << endl;)
}

void Propogator::batchProcess() {
	cout << "EVENTFILTER->BATCHPROCESS [" << tag << "] @ " << rank << endl;
}

void Propogator::streamProcess(int channel) {

	edge_map = (*(previous.begin()))->edge_map;

	D(cout << "EVENTFILTER->STREAMPROCESS [" << tag << "] @ " << rank
			<< " IN-CHANNEL " << channel << endl;)

	Message* inMessage, *outMessage;
	list<Message*>* tmpMessages = new list<Message*>();
	Serialization sede;

	WrapperUnit wrapper_unit;
	EventNode eventNode;

	int c = 0;
	int inp_cnt = 0;
	while (ALIVE) {

		pthread_mutex_lock(&listenerMutexes[channel]);

		while (inMessages[channel].empty())
			pthread_cond_wait(&listenerCondVars[channel],
					&listenerMutexes[channel]);

//		if(inMessages[channel].size()>1)
//				  cout<<tag<<" CHANNEL-"<<channel<<" BUFFER SIZE:"<<inMessages[channel].size()<<endl;
//

		while (!inMessages[channel].empty()) {
			//cout << rank << " actual debug " << tag << endl;
			inMessage = inMessages[channel].front();
			inMessages[channel].pop_front();
			tmpMessages->push_back(inMessage);
			inp_cnt++;
		}
		
		pthread_mutex_unlock(&listenerMutexes[channel]);
		while (!tmpMessages->empty()) {

			inMessage = tmpMessages->front();
			tmpMessages->pop_front();

			D(cout << "EVENTFILTER->POP MESSAGE: TAG [" << tag << "] @ " << rank
					<< " CHANNEL " << channel << " BUFFER " << inMessage->size
					<< endl;)

			sede.unwrap(inMessage);
			
			// if (inMessage->wrapper_length > 0) {
			// 	sede.unwrapFirstWU(inMessage, &wrapper_unit);
			// 	sede.printWrapper(&wrapper_unit);
			// }

			int offset = sizeof(int)
					+ (inMessage->wrapper_length * sizeof(WrapperUnit));

			// if(rank == 0 && tag == 3) cout << offset << endl;

			outMessage = new Message(inMessage->size - offset,
					inMessage->wrapper_length); // create new message with max. required capacity
			//cout << "before\n";
			memcpy(outMessage->buffer, inMessage->buffer, offset); // simply copy header from old message for now!
			outMessage->size += offset;
			//cout << "after\n";

			int event_count = (inMessage->size - offset) / sizeof(EventNode);
			// if(rank == 0 && tag == 3) cout << inMessage->size << " " << offset << endl;



			D(cout << "THROUGHPUT: " << event_count <<" @RANK-"<<rank<<" TIME: "<<(long int)MPI_Wtime()<< endl;)

			THROUGHPUT_LOG(
					datafile <<event_count<< "\t"
					<<rank<< "\t"
					<<(long int)MPI_Wtime()
					<< endl;
					)


			int i = 0, j = 0;
			//cout << rank << " " << tag << " " << event_count << endl;
			//cout << rank << " " << tag << " " << event_count <<  "chkpt\n";
			while (i < event_count) {
				// cout << "checkpoint\n";
				sede.TSPEdeserializeNode(inMessage, &eventNode,
						offset + (i * sizeof(EventNode)));

				D(cout << "  " << i << "\tevent_time: " << eventDG.event_time
						<< "\tevent_type: " << eventDG.event_type << "\t"
						<< "ad_id: " << eventDG.ad_id << endl;)

				

					sede.TSPEserializeNode(&eventNode, outMessage); // store filtered events directly in outgoing message!
//					sede.YSBdeserializeFT(outMessage, &eventFT,
//							outMessage->size - sizeof(EventFT));
					if(eventNode.tag == tag){
					//if(eventNode.tag == tag && inp_cnt == eventNode.cnt + 1){
						//assuming printing has the processing logic
                        sede.TSPEprintNode(rank, tag, &eventNode);
                    }
					j++;

				i++;
			}

			
			//cout << "FILTERED_EVENT_COUNT: " << j << endl;

			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0;
			int snd_cnt = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {
				
				int idx = n * worldSize + rank; // always keep workload on same rank

				if (PIPELINE) {

					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(outMessage);

					D(cout << "EVENTFILTER->PIPELINE MESSAGE [" << tag << "] #"
							<< c << " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessage->size << " CAP "
							<< outMessage->capacity << endl;)

					pthread_cond_signal(&(*v)->listenerCondVars[idx]);
					pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);

				} else {

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessage);

					D(cout << "EVENTFILTER->PUSHBACK MESSAGE [" << tag << "] #"
							<< c << " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessage->size << " CAP "
							<< outMessage->capacity << endl;)

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}
				


				auto it = previous.begin();
				auto mp = (*it)->edge_map;
				// if(mp.find(eventNode.op_id) != mp.end()) {

				// 	vector<pair<int,int>> sender_lst = mp[eventNode.op_id];
				// 	for(auto x: sender_lst){
						
				// 			//send the event
				// 			//snd_cnt++;
				// 			cout << "sending the event\n";
				// 			// int idx = (x.second-tag-1) * worldSize + x.first;
				// 			int idx = x.first;

				// 			pthread_mutex_lock(&senderMutexes[idx]);
				// 			outMessages[idx].push_back(outMessage);


				// 			pthread_cond_signal(&senderCondVars[idx]);
				// 			pthread_mutex_unlock(&senderMutexes[idx]);
				// 	}
				// }

				n++;
				break;
			}

			delete inMessage;
			c++;
		}

		tmpMessages->clear();
	}

	delete tmpMessages;
}
