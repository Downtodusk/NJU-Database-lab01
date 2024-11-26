/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/17.
//

#include "lru_k_replacer.h"
#include "common/config.h"
#include "../common/error.h"

#include <bits/ranges_algo.h>

namespace wsdb {

LRUKReplacer::LRUKReplacer(size_t k) : max_size_(BUFFER_POOL_SIZE), k_(k) {}

auto LRUKReplacer::Victim(frame_id_t *frame_id) -> bool {
	std::lock_guard<std::mutex> lock(latch_);

	if(cur_size_ <= 0) {
		printf("No available frame!!!\n");
		return false;
	}
	//寻找d最小的页面
	unsigned long long max_distance = 0;
	for(frame_id_t i = 0; i < max_size_; i++) {
		if(node_store_[i].IsEvictable()) {
			if(node_store_[i].GetBackwardKDistance(cur_ts_) > max_distance) {
				// std::cout << "frame: " << i << " distance: " << node_store_[i].GetBackwardKDistance(cur_ts_) <<
					// "History size: " << node_store_[i].GetHistory().size() << std::endl;
				max_distance = node_store_[i].GetBackwardKDistance(cur_ts_);
				*frame_id = i;
			}
		}
	}

	if(max_distance == std::numeric_limits<unsigned long long>::max()) {
		//处理多个页面的d都为+inf的情况
		timestamp_t min_timestamp = std::numeric_limits<timestamp_t>::max();
		for(frame_id_t i = 0; i < max_size_; i++) {
			if(node_store_[i].IsEvictable() && node_store_[i].GetBackwardKDistance(cur_ts_) == max_distance) {
				if(node_store_[i].GetFirstTimeStamp() < min_timestamp) {
					min_timestamp = node_store_[i].GetFirstTimeStamp();
					*frame_id = i;
				}
			}
		}
	}
	node_store_[*frame_id].ClearHistory();
	node_store_[*frame_id].SetEvictable(false);
	if(cur_size_ > 0) cur_size_--;

	// std::cout << "victim frame: " << *frame_id << "history size: " << node_store_[*frame_id].GetHistory().size() << std::endl;

	return true;
}

void LRUKReplacer::Pin(frame_id_t frame_id) {
	std::lock_guard<std::mutex> lock(latch_);

	auto it = node_store_.find(frame_id);
	if(it != node_store_.end()) {
		it->second.AddHistory(cur_ts_);
		cur_ts_++;
		if(it->second.IsEvictable()) {
			it->second.SetEvictable(false);
			if(cur_size_ > 0) cur_size_--;
		}
	}
	else {
		node_store_[frame_id] = LRUKNode(frame_id, k_);
		auto now_it = node_store_.find(frame_id);
		now_it->second.AddHistory(cur_ts_);
		now_it->second.SetEvictable(false);
		cur_ts_++;
		if(cur_size_ > 0) cur_size_--;
	}
}

void LRUKReplacer::Unpin(frame_id_t frame_id) {
	std::lock_guard<std::mutex> lock(latch_);
	auto it = node_store_.find(frame_id);
	if(it != node_store_.end()) {
		if(!it->second.IsEvictable()) {
			it->second.SetEvictable(true);
			cur_size_++;
		}
	}
	else {
		WSDB_THROW(WSDB_EXCEPTION_EMPTY, "not exist");
	}
}

auto LRUKReplacer::Size() -> size_t {
	std::lock_guard<std::mutex> lock(latch_);
	return cur_size_;
}

}  // namespace wsdb
