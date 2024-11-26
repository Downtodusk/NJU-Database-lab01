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
// Created by ziqi on 2024/7/27.
//

#include "page_handle.h"
#include "../../../common/error.h"
#include "storage/buffer/buffer_pool_manager.h"

namespace wsdb {
PageHandle::PageHandle(const TableHeader *tab_hdr, Page *page, char *bit_map, char *slots_mem)
    : tab_hdr_(tab_hdr), page_(page), bitmap_(bit_map), slots_mem_(slots_mem)
{
  WSDB_ASSERT(BITMAP_SIZE(tab_hdr->rec_per_page_) == tab_hdr->bitmap_size_, "bitmap size not match");
}
void PageHandle::WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update)
{
  WSDB_THROW(WSDB_EXCEPTION_EMPTY, "");
}

void PageHandle::ReadSlot(size_t slot_id, char *null_map, char *data) { WSDB_THROW(WSDB_EXCEPTION_EMPTY, ""); }
auto PageHandle::ReadChunk(const RecordSchema *chunk_schema) -> ChunkUptr { WSDB_THROW(WSDB_EXCEPTION_EMPTY, ""); }

NAryPageHandle::NAryPageHandle(const TableHeader *tab_hdr, Page *page)
    : PageHandle(
          tab_hdr, page, page->GetData() + PAGE_HEADER_SIZE, page->GetData() + PAGE_HEADER_SIZE + tab_hdr->bitmap_size_)
{}

void NAryPageHandle::WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update)
{
  WSDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  WSDB_ASSERT(BitMap::GetBit(bitmap_, slot_id) == update, fmt::format("update: {}", update));
  // a record consists of null map and data
  size_t rec_full_size = tab_hdr_->nullmap_size_ + tab_hdr_->rec_size_;
  memcpy(slots_mem_ + slot_id * rec_full_size, null_map, tab_hdr_->nullmap_size_);
  memcpy(slots_mem_ + slot_id * rec_full_size + tab_hdr_->nullmap_size_, data, tab_hdr_->rec_size_);
}

void NAryPageHandle::ReadSlot(size_t slot_id, char *null_map, char *data)
{
  WSDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  WSDB_ASSERT(BitMap::GetBit(bitmap_, slot_id) == true, "slot is empty");
  size_t rec_full_size = tab_hdr_->nullmap_size_ + tab_hdr_->rec_size_;
  memcpy(null_map, slots_mem_ + slot_id * rec_full_size, tab_hdr_->nullmap_size_);
  memcpy(data, slots_mem_ + slot_id * rec_full_size + tab_hdr_->nullmap_size_, tab_hdr_->rec_size_);
}

PAXPageHandle::PAXPageHandle(
    const TableHeader *tab_hdr, Page *page, const RecordSchema *schema, const std::vector<size_t> &offsets)
    : PageHandle(tab_hdr, page, page->GetData() + PAGE_HEADER_SIZE,
          page->GetData() + PAGE_HEADER_SIZE + tab_hdr->bitmap_size_),
      schema_(schema),
      offsets_(offsets)
{}

PAXPageHandle::~PAXPageHandle() = default;

// slot memery
// | nullmap_1, nullmap_2, ... , nullmap_n|
// | field_1_1, field_1_2, ... , field_1_n |
// | field_2_1, field_2_2, ... , field_2_n |
// ...
// | field_m_1, field_m_2, ... , field_m_n |
void PAXPageHandle::WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update)
{
	WSDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
	WSDB_ASSERT(BitMap::GetBit(bitmap_, slot_id) == update, "Invalid update operation");

	// 写入 Nullmap
	memcpy(slots_mem_ + slot_id * tab_hdr_->nullmap_size_, null_map, tab_hdr_->nullmap_size_);

	// 写入每个字段的数据到对应列区域
	for (size_t i = 0; i < schema_->GetFieldCount(); ++i) {
		size_t field_offset = offsets_[i];
		const auto &field = schema_->GetFieldAt(i);
		size_t field_size = field.field_.field_size_;

		memcpy(slots_mem_ + field_offset + slot_id * field_size, data + schema_->GetFieldOffset(i), field_size);
	}
}

void PAXPageHandle::ReadSlot(size_t slot_id, char *null_map, char *data) {
	WSDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
	WSDB_ASSERT(BitMap::GetBit(bitmap_, slot_id) == true, "Slot is empty");

	// 读取 Nullmap
	memcpy(null_map, slots_mem_ + slot_id * tab_hdr_->nullmap_size_, tab_hdr_->nullmap_size_);

	// 读取每个字段的数据
	for (size_t i = 0; i < schema_->GetFieldCount(); ++i) {
		size_t field_offset = offsets_[i];
		const auto &field = schema_->GetFieldAt(i);
		size_t field_size = field.field_.field_size_;

		memcpy(data + schema_->GetFieldOffset(i), slots_mem_ + field_offset + slot_id * field_size, field_size);
	}
}

auto PAXPageHandle::ReadChunk(const RecordSchema *chunk_schema) -> ChunkUptr
{
    std::vector<ArrayValueSptr> col_arrs;
    col_arrs.reserve(chunk_schema->GetFieldCount());
    // read data each field and construct ArrayValue

	size_t total_records = tab_hdr_->rec_per_page_;

	// 为每个字段构造数组值
	for (size_t i = 0; i < chunk_schema->GetFieldCount(); ++i) {
		auto array_value = std::make_shared<ArrayValue>();
		const auto &field = chunk_schema->GetFieldAt(i);
		size_t field_offset = offsets_[i];
		size_t field_size = field.field_.field_size_;

		for (size_t slot_id = 0; slot_id < total_records; ++slot_id) {
			if (BitMap::GetBit(bitmap_, slot_id)) {
				char *field_data = slots_mem_ + field_offset + slot_id * field_size;
				auto value = ValueFactory::CreateValue(field.field_.field_type_, field_data, field_size);
				array_value->Append(value);
			}
		}

		col_arrs.push_back(array_value);
	}

	return std::make_unique<Chunk>(chunk_schema, std::move(col_arrs));
}
}  // namespace wsdb
