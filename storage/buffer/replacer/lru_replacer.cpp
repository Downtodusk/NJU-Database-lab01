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

#include "lru_replacer.h"
#include "common/config.h"
#include "../common/error.h"
namespace wsdb {
	LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}

	auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
		std::lock_guard<std::mutex> lock(latch_);

		if(cur_size_ <= 0) {
			printf("No available frame!!!\n");
			return false;
		}
		for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
			if (it->second == true) {
				*frame_id = it->first;
				lru_hash_.erase(it->first);
				lru_list_.erase(std::next(it).base());
				if(cur_size_ > 0) {cur_size_--;}
				return true;
			}
		}
		return false;
	}

	void LRUReplacer::Pin(frame_id_t frame_id) {
		std::lock_guard<std::mutex> lock(latch_);

		auto it = lru_hash_.find(frame_id);
		if (it != lru_hash_.end()) { //在list中，调整位置
			lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
			if(it->second->second == true) {
				if(cur_size_ > 0) {cur_size_--;}
				it->second->second = false;
			}
		}
		else { //不在list中
			lru_list_.emplace_front(frame_id, false);
			lru_hash_[frame_id] = lru_list_.begin();
			if(cur_size_ > 0) {
				cur_size_--;
			}
		}
	}

	void LRUReplacer::Unpin(frame_id_t frame_id) {
		std::lock_guard<std::mutex> lock(latch_);

		auto it = lru_hash_.find(frame_id);
		if (it != lru_hash_.end()) {
			if(it->second->second == false) {
				it->second->second = true;
				cur_size_++;
			}
		}
		else {
			WSDB_THROW(WSDB_UNEXPECTED_NULL, "not exist");
		}
		// for(auto it : lru_list_) {
		// 	printf("[%d, %d],  ", it.first, it.second);
		// }
	}

	auto LRUReplacer::Size() -> size_t {
		std::lock_guard<std::mutex> lock(latch_);
		return cur_size_;
	}

}

// namespace wsdb
