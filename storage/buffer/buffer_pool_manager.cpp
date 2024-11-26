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
#include "buffer_pool_manager.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"

#include <iostream>

namespace wsdb {

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, wsdb::LogManager *log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
{
  if (REPLACER == "LRUReplacer") {
    replacer_ = std::make_unique<LRUReplacer>();
  } else if (REPLACER == "LRUKReplacer") {
    replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
  } else {
    WSDB_FETAL("Unknown replacer: " + REPLACER);
  }
  // init free_list_
  for (frame_id_t i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); i++) {
    free_list_.push_back(i);
  }
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page * {
	std::lock_guard<std::mutex> lock(latch_);

	auto page_lookup_key = fid_pid_t{fid, pid};

	// 检查页面是否已经在缓冲池中
	auto it = page_frame_lookup_.find(page_lookup_key);
	if (it != page_frame_lookup_.end()) {
		frame_id_t frame_id = it->second;
		Frame& frame = frames_[frame_id];
		frame.Pin();
		replacer_->Pin(frame_id);
		return frame.GetPage();
	}

	// 页面不在缓冲池中
	frame_id_t frame_id = GetAvailableFrame();
	UpdateFrame(frame_id, fid, pid);
	return frames_[frame_id].GetPage();
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool {
    std::lock_guard<std::mutex> lock(latch_);

    auto it = page_frame_lookup_.find({fid, pid});
    if (it == page_frame_lookup_.end()) {
        return false;
    }

    frame_id_t frame_id = it->second;
    Frame& frame = frames_[frame_id];
    if (!frame.InUse()) {
        return false;
    }
    frame.Unpin();
    if (is_dirty) {
        frame.SetDirty(true);
    }
    if (!frame.InUse()) {
        replacer_->Unpin(frame_id);
    }
    return true;
}

auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool {
	std::lock_guard<std::mutex> lock(latch_);

	auto it = page_frame_lookup_.find({fid, pid});
	if (it == page_frame_lookup_.end()) {return true;}

	frame_id_t frame_id = it->second;
	Frame &frame = frames_[frame_id];

	if(frame.InUse()) return false;

	if(frame.IsDirty()) {
		disk_manager_->WritePage(fid, pid, frame.GetPage()->GetData());
	}
	frame.Reset();
	free_list_.push_front(frame_id);
	replacer_->Unpin(frame_id);
	page_frame_lookup_.erase({fid, pid});
	// printf("Avaliable page : %ld\n", free_list_.size());
	return true;
}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool {
	//std::lock_guard<std::mutex> lock(latch_);

	bool res = false;
	for (auto it = page_frame_lookup_.begin(); it != page_frame_lookup_.end();) {
		if (it->first.fid == fid) {
			auto next_it = std::next(it);
			DeletePage(it->first.fid, it->first.pid);
			it = next_it;
		} else {
			++it;
		}
	}
	return res;
}
	/**
 * Flush the page to disk
 * 1. grant the latch
 * 2. if the page is not in the buffer, return false
 * 3. flush the page to disk if the page is dirty
 * @param fid
 * @param pid
 * @return true if the page is flushed successfully
 */

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool {
	std::lock_guard<std::mutex> lock(latch_);
	// std::cout << "In FlushPage, fid = " << fid << "  Page id = " << pid <<std::endl;
	auto frame = GetFrame(fid, pid);
	if (frame == nullptr) {return false;}
	if(frame->IsDirty()) {
		disk_manager_->WritePage(fid, pid, frame->GetPage()->GetData());
		// frame->SetDirty(false);
	}
	return true;
}

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool {
	// std::lock_guard<std::mutex> lock(latch_);

	bool res = false;
	for (auto it = page_frame_lookup_.begin(); it != page_frame_lookup_.end(); ++it) {
		if (it->first.fid == fid) {
			FlushPage(it->first.fid, it->first.pid);
			res = true;
		}
	}
	return res;
}

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t {
	if (!free_list_.empty()) {
		frame_id_t frame_id = free_list_.front();
		free_list_.pop_front();
		return frame_id;
	}
	frame_id_t frame_id = -1;
	if (replacer_->Victim(&frame_id)) {
		return frame_id;
	}
	//没有空闲页面
	throw std::runtime_error("No free frame available.");
}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid) {
	Frame& frame = frames_[frame_id];

	fid_pid_t old_page_key;
	for (const auto& [key, id] : page_frame_lookup_) {
		if (id == frame_id) {
			old_page_key = key;
			break;
		}
	}

	auto it = page_frame_lookup_.find(old_page_key);
	if (it != page_frame_lookup_.end()) {
		if (frame.IsDirty()) {
			disk_manager_->WritePage(it->first.fid, it->first.pid, frame.GetPage()->GetData());
		}
		page_frame_lookup_.erase(it);
	}

	frame.Reset();
	//需要读取数据
	disk_manager_->ReadPage(fid, pid, frame.GetPage()->GetData());
	frame.GetPage()->SetTablePageId(fid, pid);

	frame.Pin();
	replacer_->Pin(frame_id);
	page_frame_lookup_[{fid, pid}] = frame_id;
}



auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame *
{
  const auto it = page_frame_lookup_.find({fid, pid});
  return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
}

}  // namespace wsdb
