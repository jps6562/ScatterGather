////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : YOUR NAME
//   Last Modified : 
//

// Include Files
#include <stdlib.h>
#include <sg_service.h>
#include <cmpsc311_log.h>
#include <sg_defs.h>
#include <string.h>
// Project Includes
#include <sg_cache.h>

// Defines
#define FAILURE -1
int timeCount = 0, hits = 0, access = 0;

//struct cache caches* = (struct cache*)malloc(32* sizeof(struct cache)); 

void incTimeCount(){timeCount++;}
void incHits(){hits++;}
void incAccess(){access++;}
struct cache
{
    SG_Node_ID nodeID;
    SG_Block_ID blockID;
    char blockData[SG_BLOCK_SIZE];
    int timeStamp;
};
// Functional Prototypes
struct cache caches[SG_MAX_CACHE_ELEMENTS*4];
//
// Functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : initSGCache
// Description  : Initialize the cache of block elements
//
// Inputs       : maxElements - maximum number of elements allowed
// Outputs      : 0 if successful, -1 if failure

int initSGCache( uint16_t maxElements ) 
{

    for(int i = 0; i < maxElements; i++)
    {
        caches[i].nodeID = 0;
        caches[i].blockID = 0;
        for(int j = 0; j < SG_BLOCK_SIZE; j++)
        {
            caches[i].blockData[j] = 0;
        }
        caches[i].timeStamp = 0;
    }
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : closeSGCache
// Description  : Close the cache of block elements, clean up remaining data
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

int closeSGCache( void ) 
{
    logMessage(LOG_INFO_LEVEL, "Closing cache: %d queries, %d hits, (%f%% hit rate)", access, hits, ((float)hits)/((float)access)*100 );
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : getSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
// Outputs      : pointer to block or NULL if not found

char * getSGDataBlock( SG_Node_ID nde, SG_Block_ID blk ) 
{
    incAccess();
    for( int i = 0; i < SG_MAX_CACHE_ELEMENTS; i++)
    {
        if (nde == caches[i].nodeID && blk == caches[i].blockID)
        { 
            incHits(); 
            caches[i].timeStamp = timeCount; 
            incTimeCount();
            return caches[i].blockData; 
        }
        
    }
    return NULL;
    
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : putSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
//                block - block to insert into cache
// Outputs      : 0 if successful, -1 if failure

int putSGDataBlock( SG_Node_ID nde, SG_Block_ID blk, char *block ) 
{
    incAccess(); 
    int lowestIndex, lowestTime = -1;
    for (int i = 0; i < SG_MAX_CACHE_ELEMENTS; i++)
    {
        if ( lowestTime > caches[i].timeStamp || lowestTime == -1)
        {
            lowestTime = caches[i].timeStamp;
            lowestIndex = i;
        }
    }

    caches[lowestIndex].nodeID = nde;
    caches[lowestIndex].blockID = blk;
    memcpy( caches[lowestIndex].blockData, block, SG_BLOCK_SIZE);
    caches[lowestIndex].timeStamp = timeCount;
    incTimeCount();

    // Return successfully
    return( 0 );
}
