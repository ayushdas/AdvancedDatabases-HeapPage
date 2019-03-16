#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "heappage.h"
#include "heapfile.h"
#include "bufmgr.h"
#include "db.h"

using namespace std;

//------------------------------------------------------------------
// Constructor of HeapPage
//
// Input     : Page ID
// Output    : None
//------------------------------------------------------------------

void HeapPage::Init(PageID pageNo)
{
	SLOT_SET_EMPTY(slots[0]);
	this->nextPage = INVALID_PAGE;
	this->prevPage = INVALID_PAGE;
	this->numOfSlots = 0; // initially 0 slots
	this->pid = pageNo;
	this->fillPtr = HEAPPAGE_DATA_SIZE;
	this->freeSpace = HEAPPAGE_DATA_SIZE;
}

void HeapPage::SetNextPage(PageID pageNo)
{
	this->nextPage = pageNo;
}

void HeapPage::SetPrevPage(PageID pageNo)
{
	this->prevPage = pageNo;
}

PageID HeapPage::GetNextPage()
{
	return nextPage;
}

PageID HeapPage::GetPrevPage()
{
	return prevPage;
}


//------------------------------------------------------------------
// HeapPage::InsertRecord
//
// Input     : Pointer to the record and the record's length 
// Output    : Record ID of the record inserted.
// Purpose   : Insert a record into the page
// Return    : OK if everything went OK, DONE if sufficient space 
//             does not exist
//------------------------------------------------------------------

Status HeapPage::InsertRecord(char *recPtr, int length, RecordID& rid)
{
	int slotNumber = 0;
	bool emptySlot = false;
	while (slotNumber < numOfSlots){
		if (SLOT_IS_EMPTY(slots[slotNumber])){
			emptySlot = true;
			break;
		}
		slotNumber += 1;
	}
	if (emptySlot){
		if (length > freeSpace){ 
			return DONE;
		}
		else{
			SLOT_FILL(slots[slotNumber],fillPtr - length,length);
		}
	}
	else{
		int total_size = 0;
		total_size = sizeof(Slot) + length; 
		if (total_size > freeSpace){
			return DONE;
		}
		else{
			SLOT_FILL(slots[slotNumber],fillPtr - length,length);
			numOfSlots += 1;
			freeSpace -= sizeof(Slot);
		}
	}
	memcpy(&data[fillPtr - length],recPtr,length);
	freeSpace -= length; 
	fillPtr -= length;
	rid.slotNo = slotNumber;
	rid.pageNo = pid;
	return OK;
}


//------------------------------------------------------------------
// HeapPage::DeleteRecord 
//
// Input    : Record ID
// Output   : None
// Purpose  : Delete a record from the page
// Return   : OK if successful, FAIL otherwise  
//------------------------------------------------------------------ 

Status HeapPage::DeleteRecord(const RecordID& rid)
{
  if(SLOT_IS_EMPTY(slots[rid.slotNo])){
	  return FAIL;
  }
  if(numOfSlots <= rid.slotNo){
  return FAIL;
  }
  int slotOffset = slots[rid.slotNo].offset;
  int slotLength = slots[rid.slotNo].length;
  int slot_index = 0;
  while(slot_index < numOfSlots){
    if(!SLOT_IS_EMPTY(slots[slot_index]) && (slots[slot_index].offset < slotOffset)){
      slots[slot_index].offset += slotLength;
    }
    slot_index++;
  }
  if(rid.slotNo == numOfSlots - 1){
    numOfSlots -= 1;
    freeSpace += sizeof(Slot);
  }
  memmove( &(data[fillPtr + slotLength]), &(data[fillPtr]), slotOffset - fillPtr);
  SLOT_SET_EMPTY(slots[rid.slotNo]);
  return OK;
}


//------------------------------------------------------------------
// HeapPage::FirstRecord
//
// Input    : None
// Output   : record id of the first record on a page
// Purpose  : To find the first record on a page
// Return   : OK if successful, DONE otherwise
//------------------------------------------------------------------

Status HeapPage::FirstRecord(RecordID& rid)
{
	if (IsEmpty() == true)
		return DONE;
	int allSlots = 0;
	while(allSlots < numOfSlots){
		if(!SLOT_IS_EMPTY(slots[allSlots])){
			rid.pageNo = pid;
			rid.slotNo = allSlots;
			return OK;
		}
		allSlots += 1;
	}
	return DONE;
}


//------------------------------------------------------------------
// HeapPage::NextRecord
//
// Input    : ID of the current record
// Output   : ID of the next record
// Return   : Return DONE if no more records exist on the page; 
//            otherwise OK. In case of an error, return FAIL.
//------------------------------------------------------------------

Status HeapPage::NextRecord(RecordID curRid, RecordID& nextRid)
{
	if (numOfSlots <= curRid.slotNo){
		return DONE;
	}
	else{
		int nextSlot = curRid.slotNo + 1;
		while(nextSlot < numOfSlots){
			if (nextSlot >= numOfSlots){
				return DONE;
			}
			else{
				if (SLOT_IS_EMPTY(slots[nextSlot])){
					nextSlot++;
					continue;
				}
				else { 
					nextRid.pageNo = pid;
					nextRid.slotNo = nextSlot;
					return OK;
				}
			}
		nextSlot++;	
		}
	}
	return DONE;
}


//------------------------------------------------------------------
// HeapPage::GetRecord
//
// Input    : Record ID
// Output   : Records length and a copy of the record itself
// Purpose  : To retrieve a _copy_ of a record with ID rid from a page
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::GetRecord(RecordID rid, char *recPtr, int& length)
{
	if (numOfSlots <= rid.slotNo){
		return FAIL;
	}
	else{
		int len = slots[rid.slotNo].length;
		int offset = slots[rid.slotNo].offset;
		memcpy(recPtr,&(data[offset]),len);
		length = len;
		return OK;
	}
}


//------------------------------------------------------------------
// HeapPage::ReturnRecord
//
// Input    : Record ID
// Output   : pointer to the record, record's length
// Purpose  : To output a _pointer_ to the record
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::ReturnRecord(RecordID rid, char*& recPtr, int& length)
{
	if (numOfSlots <= rid.slotNo){
		return FAIL;
	}
	else{
		int len = slots[rid.slotNo].length;
		int offset = slots[rid.slotNo].offset;
		recPtr = &data[offset];
		length = len;
		return OK;
	}
}


//------------------------------------------------------------------
// HeapPage::AvailableSpace
//
// Input    : None
// Output   : None
// Purpose  : To return the amount of available space
// Return   : The amount of available space on the heap file page.
//------------------------------------------------------------------

int HeapPage::AvailableSpace(void)
{
	if (GetNumOfRecords() < numOfSlots){
		return freeSpace;
	}
	else{
		return (freeSpace - sizeof(Slot));
	}
}


//------------------------------------------------------------------
// HeapPage::IsEmpty
// 
// Input    : None
// Output   : None
// Purpose  : Check if there is any record in the page.
// Return   : true if the HeapPage is empty, and false otherwise.
//------------------------------------------------------------------

bool HeapPage::IsEmpty(void)
{
	if (GetNumOfRecords() == 0){
		return true;
	}
	else{
		return false;
	}
}


//------------------------------------------------------------------
// HeapPage::GetNumOfRecords
// 
// Input    : None
// Output   : None
// Purpose  : To return the number of records in the page.
// Return   : The number of records currently stored in the page.
//------------------------------------------------------------------

int HeapPage::GetNumOfRecords()
{
	int valid_slots = 0;
	int all_slots = 0;
	while (all_slots < numOfSlots){
		if(!SLOT_IS_EMPTY(slots[all_slots])){
			valid_slots += 1;
		}
		all_slots += 1;
	}
	return valid_slots;
}


//------------------------------------------------------------------
// HeapPage::CompactSlotDir
// 
// Input    : None
// Output   : None
// Purpose  : To compact the slot directory (eliminate empty slots).
// Return   : None
//------------------------------------------------------------------

void HeapPage::CompactSlotDir()
{
  int invalidSlots = 0; 
  int move_behind_steps = 0;
  for(int i = 0; i < numOfSlots; i++){
    if(SLOT_IS_EMPTY( slots[i])){
      invalidSlots++;
    }else{
	  move_behind_steps = i - invalidSlots;
      slots[move_behind_steps].offset = slots[i].offset; 
      slots[move_behind_steps].length = slots[i].length;
    }
  }
  numOfSlots -= invalidSlots;
  long total_free_space = invalidSlots * sizeof(Slot);
  freeSpace +=  total_free_space;
}
