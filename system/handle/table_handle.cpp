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
// Created by ziqi on 2024/7/19.
//

#include "table_handle.h"
namespace wsdb {

TableHandle::TableHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t table_id,
    TableHeader &hdr, RecordSchemaUptr &schema, StorageModel storage_model)
    : tab_hdr_(hdr),
      table_id_(table_id),
      disk_manager_(disk_manager),
      buffer_pool_manager_(buffer_pool_manager),
      schema_(std::move(schema)),
      storage_model_(storage_model)
{
  // set table id for table handle;
  schema_->SetTableId(table_id_);
  if (storage_model_ == PAX_MODEL) {
   field_offset_.resize(schema_->GetFieldCount());
   // calculate offsets of fields
  	// WSDB_STUDENT_TODO(l1, f2);
    size_t cur_offset = tab_hdr_.nullmap_size_;
  	for (size_t i = 0; i < schema_->GetFieldCount(); i++) {
  		field_offset_[i] = cur_offset;
  		//获取Field_i这一列的每一个项的大小
  		size_t field_size = schema_->GetFieldAt(i).field_.field_size_;
  		cur_offset += field_size * tab_hdr_.rec_per_page_;
  	}
  }
}

/**
 * Get a record by rid
 * 1. fetch the page handle by rid
 * 2. check if there is a record in the slot using bitmap, if not, unpin the page and throw WSDB_RECORD_MISS
 * 3. read the record from the slot using page handle
 * 4. unpin the page
 * @param rid
 * @return record
 */

auto TableHandle::GetRecord(const RID &rid) -> RecordUptr
{
	// std::cout << "brkpoint1" << std::endl;
	auto nullmap = std::make_unique<char[]>(tab_hdr_.nullmap_size_);
    auto data    = std::make_unique<char[]>(tab_hdr_.rec_size_);
    auto page_handle = FetchPageHandle(rid.PageID());
	auto bitmap = page_handle->GetBitmap();
	//不存在记录
	// std::cout << "brkpoint2" << std::endl;
	if(!BitMap::GetBit(bitmap, rid.SlotID())) {
		buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
		WSDB_THROW(WSDB_RECORD_MISS, "record miss!");
	}
	//对应记录存在
	// std::cout << "brkpoint3" << std::endl;
	page_handle->ReadSlot(rid.SlotID(), nullmap.get(), data.get());
	buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
	// std::cout << "brkpoint4" << data.get() << std::endl;

	// Record rec = Record(schema_.get(), nullmap.get(), data.get(), rid);
	auto record = std::make_unique<Record>(schema_.get(), nullmap.get(), data.get(), rid);
	// std::cout << "brkpoint5" << std::endl;
	// return RecordUptr(&rec);
	return record;
}

auto TableHandle::GetChunk(page_id_t pid, const RecordSchema *chunk_schema) -> ChunkUptr {
	// 1. 获取页面句柄
	auto page_handle = FetchPageHandle(pid);
	if (!page_handle) {
		WSDB_THROW(WSDB_PAGE_MISS, fmt::format("Failed to fetch page handle for page id {}", pid));
	}
	// 2. 从页面读取 Chunk
	auto chunk = page_handle->ReadChunk(chunk_schema);

	// 3. 释放页面
	buffer_pool_manager_->UnpinPage(table_id_, pid, false);

	return chunk;
}
	/**
	 * Insert a record into the table
	 * 1. create a page handle using CreatePageHandle
	 * 2. get an empty slot in the page
	 * 3. write the record into the slot
	 * 4. update the bitmap and the number of records in the page header
	 * 5. if the page is full after inserting the record, update the first free page id in the file header and set the
	 * next page id of the current page
	 * 6. unpin the page
	 * @param record
	 * @return rid of the inserted record
	 */
auto TableHandle::InsertRecord(const Record &record) -> RID {
	auto page_handle = CreatePageHandle();
	auto bitmap = page_handle->GetBitmap();

	size_t slot_id = BitMap::FindFirst(bitmap, tab_hdr_.rec_per_page_, 0, false);
	// std::cout << "slot_id: " << slot_id << "  total_rec_num: " << tab_hdr_.rec_per_page_ << std::endl;
	if (slot_id == tab_hdr_.rec_per_page_) {
		buffer_pool_manager_->UnpinPage(table_id_, page_handle->GetPage()->GetPageId(), true);
		WSDB_THROW(WSDB_EXCEPTION_EMPTY, "No free slot in page");
	}

	page_handle->WriteSlot(slot_id, record.GetNullMap(), record.GetData(), false);

	BitMap::SetBit(bitmap, slot_id, true);
	auto recordNum = page_handle->GetPage()->GetRecordNum();
	page_handle->GetPage()->SetRecordNum(recordNum + 1);
	// std::cout << "recordNum: " << page_handle->GetPage()->GetRecordNum() << std::endl;
	//检查页面是否已满
	if(page_handle->GetPage()->GetRecordNum() == tab_hdr_.rec_per_page_) {
		tab_hdr_.first_free_page_ = page_handle->GetNextPageId();
		page_handle->SetNextPageId(INVALID_PAGE_ID);
	}

	buffer_pool_manager_->UnpinPage(table_id_, page_handle->GetPage()->GetPageId(), true);
	return {page_handle->GetPage()->GetPageId(), (slot_id_t)slot_id};
}

/**
 * Insert a record into the table given rid
 * 1. if rid is invalid, unpin the page and throw WSDB_PAGE_MISS
 * 2. fetch the page handle and check the bitmap, if the slot is not empty, throw WSDB_RECORD_EXISTS
 * 3. do the rest of the steps in InsertRecord 3-6
 * @param rid
 * @param record
 */
void TableHandle::InsertRecord(const RID &rid, const Record &record)
{
  if (rid.PageID() == INVALID_PAGE_ID) {
  	buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
    WSDB_THROW(WSDB_PAGE_MISS, fmt::format("Page: {}", rid.PageID()));
  }
  auto page_hdr = FetchPageHandle(rid.PageID());
	auto bitmap = page_hdr->GetBitmap();

	if(BitMap::GetBit(bitmap, rid.SlotID())) {
		WSDB_THROW(WSDB_RECORD_MISS, "record already exists!!!");
	}

	page_hdr->WriteSlot(rid.SlotID(), record.GetNullMap(), record.GetData(), false);

	BitMap::SetBit(bitmap, rid.SlotID(), true);
	auto recordNum = page_hdr->GetPage()->GetRecordNum();
	page_hdr->GetPage()->SetRecordNum(recordNum + 1);
	//检查页面是否已满
	if(page_hdr->GetPage()->GetRecordNum() == tab_hdr_.rec_per_page_) {
		tab_hdr_.first_free_page_ = page_hdr->GetNextPageId();
		page_hdr->SetNextPageId(INVALID_PAGE_ID);
	}

	buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), true);
}

	/**
	 * Delete the record by rid
	 * 1. if the slot is empty, unpin the page and throw WSDB_RECORD_MISS
	 * 2. update the bitmap and the number of records in the page header
	 * 3. if the page is not full after deleting the record, update the first free page id in the file header and the next
	 * page id in the page header
	 * 4. unpin the page
	 * @param rid
	 */

void TableHandle::DeleteRecord(const RID &rid) {
	auto page_hdr = FetchPageHandle(rid.PageID());
	auto bitmap = page_hdr->GetBitmap();
	if(!BitMap::GetBit(bitmap, rid.SlotID())) {
		buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
		WSDB_THROW(WSDB_RECORD_MISS, "No slot in page");
	}
	BitMap::SetBit(bitmap, rid.SlotID(), false);
	auto recordNum = page_hdr->GetPage()->GetRecordNum();
	page_hdr->GetPage()->SetRecordNum(recordNum - 1);
	if(page_hdr->GetPage()->GetRecordNum() != tab_hdr_.rec_per_page_) {
		page_hdr->SetNextPageId(tab_hdr_.first_free_page_);
		tab_hdr_.first_free_page_ = rid.PageID();
	}
	buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), true);
}

	/**
 * Update the record by rid
 * 1. if the slot is empty, unpin the page and throw WSDB_RECORD_MISS
 * 2. write slot
 * 3. unpin the page
 * @param rid
 * @param record
 */

void TableHandle::UpdateRecord(const RID &rid, const Record &record) {
	auto page_hdr = FetchPageHandle(rid.PageID());
	auto bitmap = page_hdr->GetBitmap();
	if(!BitMap::GetBit(bitmap, rid.SlotID())) {
		buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
		WSDB_THROW(WSDB_RECORD_MISS, "Record not exists!!!");
	}
	page_hdr->WriteSlot(rid.SlotID(), record.GetNullMap(), record.GetData(), true);
	// BitMap::SetBit(bitmap, rid.SlotID(), true);
	buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), true);
}

auto TableHandle::FetchPageHandle(page_id_t page_id) -> PageHandleUptr
{
  auto page = buffer_pool_manager_->FetchPage(table_id_, page_id);
  return WrapPageHandle(page);
}

auto TableHandle::CreatePageHandle() -> PageHandleUptr
{
  if (tab_hdr_.first_free_page_ == INVALID_PAGE_ID) {
    return CreateNewPageHandle();
  }
  auto page = buffer_pool_manager_->FetchPage(table_id_, tab_hdr_.first_free_page_);
  return WrapPageHandle(page);
}

auto TableHandle::CreateNewPageHandle() -> PageHandleUptr
{
  auto page_id = static_cast<page_id_t>(tab_hdr_.page_num_);
  tab_hdr_.page_num_++;
  auto page   = buffer_pool_manager_->FetchPage(table_id_, page_id);
  auto pg_hdl = WrapPageHandle(page);
  pg_hdl->SetNextPageId(tab_hdr_.first_free_page_);
  tab_hdr_.first_free_page_ = page_id;
  return pg_hdl;
}

auto TableHandle::WrapPageHandle(Page *page) -> PageHandleUptr
{
  switch (storage_model_) {
    case StorageModel::NARY_MODEL: return std::make_unique<NAryPageHandle>(&tab_hdr_, page);
    case StorageModel::PAX_MODEL: return std::make_unique<PAXPageHandle>(&tab_hdr_, page, schema_.get(), field_offset_);
    default: WSDB_FETAL("Unknown storage model");
  }
}

auto TableHandle::GetTableId() const -> table_id_t { return table_id_; }

auto TableHandle::GetTableHeader() const -> const TableHeader & { return tab_hdr_; }

auto TableHandle::GetSchema() const -> const RecordSchema & { return *schema_; }

auto TableHandle::GetTableName() const -> std::string
{
  auto file_name = disk_manager_->GetFileName(table_id_);
  return OBJNAME_FROM_FILENAME(file_name);
}

auto TableHandle::GetStorageModel() const -> StorageModel { return storage_model_; }

auto TableHandle::GetFirstRID() -> RID
{
  auto page_id = FILE_HEADER_PAGE_ID + 1;
  while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
    auto pg_hdl = FetchPageHandle(page_id);
    auto id     = BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, 0, true);
    if (id != tab_hdr_.rec_per_page_) {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      return {page_id, static_cast<slot_id_t>(id)};
    }
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    page_id++;
  }
  return INVALID_RID;
}

auto TableHandle::GetNextRID(const RID &rid) -> RID
{
  auto page_id = rid.PageID();
  auto slot_id = rid.SlotID();
  while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
    auto pg_hdl = FetchPageHandle(page_id);
    slot_id = static_cast<slot_id_t>(BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, slot_id + 1, true));
    if (slot_id == static_cast<slot_id_t>(tab_hdr_.rec_per_page_)) {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      page_id++;
      slot_id = -1;
    } else {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      return {page_id, static_cast<slot_id_t>(slot_id)};
    }
  }
  return INVALID_RID;
}

auto TableHandle::HasField(const std::string &field_name) const -> bool
{
  return schema_->HasField(table_id_, field_name);
}

}  // namespace wsdb
