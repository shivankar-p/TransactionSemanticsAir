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
 * EventGenerator.cpp
 *
 *  Created on: Dec 06, 2018
 *      Author: vinu.venugopal
 */

#include <mpi.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <iterator>
#include <list>
#include <string>
#include <vector>
#include <sstream>
#include <regex>

#include "../communication/Window.hpp"
#include "Node.hpp"

using namespace std;

Node::Node(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
    //assuming each file will have its rank message already constructed.
	std::ifstream ifile("../TSPE_data/rank"+to_string(rank)+".txt");
	//only 1 line
	
	for (std::string line; getline(ifile, line);) {

		msg = line;
	}

	//initializing memory
	mem['A'] = 500;
	mem['B'] = 170;

	cout << "AIR INSTANCE AT RANK " << (rank + 1) << "/" << worldSize <<  " | MSG/SEC/RANK: " << PER_SEC_MSG_COUNT << " | AGGR_WINDOW: " << AGG_WIND_SPAN << "ms" << endl;

	// S_CHECK(
	// 		datafile.open("Data/data" + to_string(rank) + ".tsv");
	// )

	D(cout << "EVENTGENERATOR [" << tag << "] CREATED @ " << rank << endl;)
}

Node::~Node() {
	D(cout << "EVENTGENERATOR [" << tag << "] DELTETED @ " << rank << endl;)
}

void Node::batchProcess() {
	D(
			cout << "EVENTGENERATOR->BATCHPROCESS: TAG [" << tag << "] @ "
			<< rank << endl
			;)
}

void Node::streamProcess(int channel) {

	D(
			cout << "EVENTGENERATOR->STREAMPROCESS: TAG [" << tag << "] @ "
			<< rank << " CHANNEL " << channel << endl
			;)


	Message* message;
	Message** outMessagesPerSec = new Message*[PER_SEC_MSG_COUNT];

	WrapperUnit wrapper_unit;
	EventNode eventNode;

	int wrappers_per_msg = 1; // currently only one wrapper per message!
	//int events_per_msg = this->throughput / PER_SEC_MSG_COUNT / worldSize;
	string dum = msg;
	std::istringstream ss(dum);
	int events_per_msg = 0;

	// Loop through the string, splitting at ':'
    while (std::getline(ss, dum, ':')) {
        events_per_msg++;
    }


	long int start_time = (long int) MPI_Wtime();
	long int t1, t2;

	int iteration_count = 0, c = 0;

	while (iteration_count != 1) {

//		cout << "\n_________Iteration: " << iteration_count
//				<< ", Local-throughput: " << (THROUGHPUT / worldSize)
//				<< ", Start-time: " << (start_time * 1000)
//				<< ", Per-Sec-Msg-Count: " << PER_SEC_MSG_COUNT << endl;

		t1 = MPI_Wtime();

		int msg_count = 0;
		while (msg_count < PER_SEC_MSG_COUNT) {

			outMessagesPerSec[msg_count] = new Message(
					events_per_msg * sizeof(EventNode), wrappers_per_msg);

			// Message header
			long int time_now = (start_time + iteration_count) * 1000;
			wrapper_unit.window_start_time = time_now + 999; // this is the max-event-end-time


			memcpy(outMessagesPerSec[msg_count]->buffer, &wrappers_per_msg,
					sizeof(int));
			memcpy(outMessagesPerSec[msg_count]->buffer + sizeof(int),
					&wrapper_unit, sizeof(WrapperUnit));
			outMessagesPerSec[msg_count]->size += sizeof(int)
					+ outMessagesPerSec[msg_count]->wrapper_length
							* sizeof(WrapperUnit);


			
			getNextMessage(&eventNode, &wrapper_unit,
					outMessagesPerSec[msg_count], events_per_msg, time_now);


			//assuming message will be released of passed only after num == denominator
			wrapper_unit.completeness_tag_numerator = 1;
			wrapper_unit.completeness_tag_denominator = 1 + eventNode.cnt;

			// Debug output ---
			Serialization sede;
//			WrapperUnit wu;
//			sede.unwrapFirstWU(message, &wu); //index starts from one
//			sede.printWrapper(&wu);
//

			for (int e = 0; e < events_per_msg; e++) {
				sede.TSPEdeserializeNode(outMessagesPerSec[msg_count], &eventNode,
						sizeof(int)
								+ (outMessagesPerSec[msg_count]->wrapper_length
										* sizeof(WrapperUnit))
								+ (e * sizeof(EventNode)));
				//we perform logic only if it matches the tag
				if(eventNode.tag == tag ){
					sede.TSPEprintNode(rank, tag, &eventNode);
				}
			}
		D(	cout << "message_size: " << outMessagesPerSec[msg_count]->size
					<< "\tmessage_capacity: "
					<< outMessagesPerSec[msg_count]->capacity << endl;)
			// ----

			msg_count++;
			c++;
		}

		t2 = MPI_Wtime();
		while ((t2 - t1) < 1) {
			usleep(100);
			t2 = MPI_Wtime();
		}

		msg_count = 0;
		while (msg_count < PER_SEC_MSG_COUNT) {
			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {

				int idx = n * worldSize + rank; // always keep workload on same rank

				// cout << rank << " " << tag << " " << (*v)->rank << " " << (*v)->tag << endl;
				// cout << idx << endl;

				if (PIPELINE) {

					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(
							outMessagesPerSec[msg_count]);

					D(
							cout << "EVENTGENERATOR->PIPELINE MESSAGE [" << tag
							<< "] #" << c << " @ " << rank
							<< " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessagesPerSec[msg_count]->size << " CAP "
							<< outMessagesPerSec[msg_count]->capacity << endl
							;)

					pthread_cond_signal(&(*v)->listenerCondVars[idx]);
					pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);

				} else {

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessagesPerSec[msg_count]);

					D(
							cout << "EVENTGENERATOR->PUSHBACK MESSAGE [" << tag
							<< "] #" << c << " @ " << rank
							<< " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessagesPerSec[msg_count]->size << " CAP "
							<< outMessagesPerSec[msg_count]->capacity << endl
							;)

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}

				n++;
				break; // only one successor node allowed!
			}

			msg_count++;
		}

		iteration_count++;
	}
}

void Node::getNextMessage(EventNode* event, WrapperUnit* wrapper_unit,
		Message* message, int events_per_msg, long int time_now) {

	Serialization sede;

	std::istringstream ss(msg);
    std::vector<std::vector<std::string>> parts;
    std::string part;


    
	while (std::getline(ss, part, ':')) {
		
        std::vector<std::string> subparts;

        // Further split each part by ','
        std::istringstream partStream(part);
        std::string subpart;

        while (std::getline(partStream, subpart, ',')) {
            subparts.push_back(subpart);
        }

        parts.push_back(subparts);
    }

	// for(int i = 0; i < parts.size(); i++) {
	// 	for(int j = 0; j < 5; j++) cout << parts[i][j] << " ";
	// 	cout << endl;
	// }

	// string rnd = randomstr(50);
	// const char* rndptr = rnd.c_str();
	// memcpy(event->ad_id, "3192274f-32f1-442b-8fc0-d5491664a447\0", 37); //default ad_id that would be replaced later
	// memcpy(event->userid_pageid_ipaddress,
	// 		rndptr,
	// 		50); //default values that would be used for all the events

	long int max_time = 0;

	// Serializing the events
	
	int i = 0;
	while (i < events_per_msg) {


		memcpy(event->op_id, parts[i][0].c_str(), 5);
		memcpy(event->func, parts[i][1].c_str(), 20);
		event->tag = std::stol(parts[i][2]);

		string edge_str = parts[i][3];

		if(edge_str != "")
		{
			std::vector<std::pair<int, int>> pairs;

    		
    		std::regex pattern("\\[(\\d+)_(\\d+)\\]");
    		std::sregex_iterator it(edge_str.begin(), edge_str.end(), pattern);
    		std::sregex_iterator end;

    	
    		for (; it != end; ++it) {
    		    int first = std::stoi(it->str(1));
    		    int second = std::stoi(it->str(2));
				//cout << first << " " << second << endl;
    		    pairs.emplace_back(first, second);
    		}

			edge_map[event->op_id] = pairs;
		}
		event->cnt = std::stol(parts[i][4]);
		//cout << event->cnt << endl;
		

		//event->event_time = time_now + (999 - i % 1000); // uniformly distribute event times among current message window, upper first
		//event->event_time = (long int) (MPI_Wtime() * 1000);

		// S_CHECK(
		// 	datafile << event->event_time << "\t"
		// 			//divide this by the agg wid size
		// 			<< event->event_time / AGG_WIND_SPAN << "\t"
		// 			//divide this by the agg wid size
		// 			<< rank << "\t" << i << "\t" << event->event_type << "\t"
		// 			<< event->ad_id << endl;
		// );
		//cout << "ser called\n";
		sede.TSPEserializeNode(event, message);

		// if (max_time < event->event_time)
		// 	max_time = event->event_time;

		i++;
	}

	wrapper_unit->window_start_time = max_time;
}
