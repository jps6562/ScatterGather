////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : 
//   Last Modified : 
//

// Include Files

// Project Includes
#include <sg_driver.h>
#include <sg_service.h>
#include <string.h>
#include <stdlib.h>
#include <sg_cache.h>

// Defines
#define FAILURE -1
#define SUCCESS 0

struct sg_block
{
    SG_Node_ID nodeRemID;
    SG_Block_ID blkID;
};

struct sg_file
{
    struct sg_block blocks[SG_MAX_BLOCKS_PER_FILE*2];
    uint32_t fileSize;
    uint32_t filePos;
    char filePath[1024];
    SgFHandle fileHandle;
};



//remote sequence linked list
struct remoteNode
{
    SG_Node_ID remoteID;
    SG_SeqNum  remoteSequence;
    struct remoteNode *nextRemoteNode;
}; 
struct remoteNode *remoteHead = NULL;

void appendRemoteNode(SG_Node_ID rem, SG_SeqNum rseq)
{
    //allocate node
    struct remoteNode *newNode = (struct remoteNode*) malloc(sizeof(struct remoteNode));
    
    //copy data
    newNode->nextRemoteNode = NULL;
    newNode->remoteID = rem;
    newNode->remoteSequence = rseq; 

    struct remoteNode *tempptr = remoteHead;

    //check to see if list is empty
    //if so seat head to node
    //if not go to end and add node
    if( NULL != remoteHead)
    {
        while( NULL != tempptr->nextRemoteNode)
        {
            tempptr = tempptr->nextRemoteNode;
        }
        tempptr->nextRemoteNode = newNode;
        
    }
    else
    {
        remoteHead = newNode;
    }
    //free(tempptr);
}

struct remoteNode *getRemoteNode( SG_Node_ID rem )
{
    //if(NULL == remoteHead){return NULL;}

    struct remoteNode *temp = remoteHead;

    while( temp != NULL )
    {
        if( temp->remoteID == rem){ break; }
        temp = temp->nextRemoteNode; 
    }
    return temp;
}

void updateSeqNumber( SG_Node_ID rem, SG_SeqNum rseq)
{
    struct remoteNode *temp = getRemoteNode(rem); 
    if(temp != NULL)
    {
        temp->remoteSequence = rseq; 
        //temp->remoteSequence = temp->remoteSequence + 1;
    }
    else 
    {
        appendRemoteNode(rem, rseq);
    }
}

//end rseq linked list


//file linked list
struct nodeFile
{ 
    struct sg_file file;
    struct nodeFile *prevNode;
    struct nodeFile *nextNode;
};
struct nodeFile *fileHead = NULL;


// Function     : appendFile
// Description  : adds a nodeFile onto the back of the linked list
//
// Inputs       : file data
// Outputs      : 
void appendFile(struct sg_file fileIn)
{
    struct nodeFile *newNode = (struct nodeFile*) malloc(sizeof(struct nodeFile));
    newNode->prevNode = NULL;
    newNode->nextNode = NULL;
    struct nodeFile *tempptr = fileHead;

    //blocks dynamically allocate
    //dont think i have to do blocks here b/c blocks dont exist b/c this is opening files
    //newNode->key = fileIn.fileHandle; 
    newNode->file.fileSize = fileIn.fileSize;
    newNode->file.filePos = fileIn.filePos;
    memcpy(newNode->file.filePath, fileIn.filePath, 1024);
    newNode->file.fileHandle = fileIn.fileHandle;

    //check to see if list is empty
    //if so seat head to node
    //if not go to end and add node
    if( NULL != fileHead)
    {
        while( NULL != tempptr->nextNode)
        {
            tempptr = tempptr->nextNode;
        }
        tempptr->nextNode = newNode;
        newNode->prevNode = tempptr;

    }
    else
    {
        
        fileHead = newNode;
    }
    //free(tempptr);
}

// Function     : getLastFile
// Description  : sets input ptr to the last existing spot in the linked list
//
// Inputs       : empty nodeFile ptr to be filled 
// Outputs      : 0 for success
int getLastFile(struct nodeFile *last )
{
    last = fileHead;
    while( NULL != last->nextNode)
    {
        last = last->nextNode;
    }
    return SUCCESS;
}

// Function     : getNodeFile
// Description  :gets the File with the associated fileHandle
//
// Inputs       : file handle and an empty ptr to a nodeFile
// Outputs      : 0 for success
struct nodeFile *getNodeFile( SgFHandle fh)
{
    struct nodeFile *temp = fileHead; 
    while( NULL != temp)
    {
        if( temp->file.fileHandle == fh){ break; }
        temp = temp->nextNode; 
    }
    return temp; 
} 

// end of file linked list


//
// Global Data
int nextFile = 0;
int localSeq, remoteSeq;

// Driver file entry

// Global data
int sgDriverInitialized = 0; // The flag indicating the driver initialized
SG_Block_ID sgLocalNodeId;   // The local node identifier
SG_SeqNum sgLocalSeqno;      // The local sequence number

// Driver support functions
int sgInitEndpoint( void ); // Initialize the endpoint

//
// Functions
////////////////////////////////////////////////////////////////////////////////

// Function     : incNextFile
// Description  : increments the variable nextFile
//
// Inputs       : 
// Outputs      : 
void incNextFile(){ nextFile++; }

// Function     : incLocalSeq
// Description  : increments the variable LocalSeq
//
// Inputs       : 
// Outputs      : 
void incLocalSeq(){ localSeq++; }

// Function     : incRemoteSeq
// Description  : increments the variable remoteSeq
//
// Inputs       : 
// Outputs      : 
void incRemoteSeq(){ remoteSeq++; }

// Function     : cache
// Description  : interacts with the cache
//
// Inputs       : node id, block id, and block data
// Outputs      : 
void cache( SG_Node_ID nde, SG_Block_ID blk, char *block )
{
    
    if ( NULL == getSGDataBlock( nde, blk))
    {
        putSGDataBlock( nde, blk, block ); 
    }
}
//
// File system interface implementation

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgopen
// Description  : Open the file for for reading and writing
//
// Inputs       : path - the path/filename of the file to be read
// Outputs      : file handle if successful test, -1 if failure

SgFHandle sgopen(const char *path) {

    localSeq = SG_INITIAL_SEQNO;
    //remoteSeq = SG_INITIAL_SEQNO;
    // First check to see if we have been initialized
    if (!sgDriverInitialized) {

        // Call the endpoint initialization 
        if ( sgInitEndpoint() ) {
            logMessage( LOG_ERROR_LEVEL, "sgopen: Scatter/Gather endpoint initialization failed." );
            return( FAILURE );
        }

        // Set to initialized
        sgDriverInitialized = 1;
    }
    initSGCache( 32 ); 
    
    struct sg_file file;

    //Populate metadata array
    strcpy( file.filePath, path );
    file.fileHandle = nextFile;
    file.filePos = 0;
    file.fileSize = 0;
    appendFile(file);
    


    incNextFile();

    //Log Success
    logMessage( LOG_INFO_LEVEL, "Completed sgOpen Succesfully");
    
    // Return the file handle 
    return( file.fileHandle );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgread
// Description  : Read data from the file
//
// Inputs       : fh - file handle for the file to read from
//                buf - place to put the data
//                len - the length of the read
// Outputs      : number of bytes read, -1 if failure

int sgread(SgFHandle fh, char *buf, size_t len) {

    logMessage(LOG_OUTPUT_LEVEL, "sgRead: start");
    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_DATA_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;
    SGDataBlock des_data_block_out;
    int blkIndex;

    //failure condition checking
    if ( fh >= nextFile || 0 > fh  ) { return FAILURE; }
    if ( getNodeFile(fh)->file.filePos >= getNodeFile(fh)->file.fileSize ) { return FAILURE; }

    blkIndex = ( getNodeFile(fh)->file.filePos / (int)SG_BLOCK_SIZE );
    
    // Setup the packet
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( sgLocalNodeId,                             // Local ID
                                    getNodeFile(fh)->file.blocks[blkIndex].nodeRemID,    // Remote ID
                                    getNodeFile(fh)->file.blocks[blkIndex].blkID,        // Block ID
                                    SG_OBTAIN_BLOCK,                            // Operation
                                    localSeq,                           // Sender sequence number
                                    getRemoteNode(getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.filePos / SG_BLOCK_SIZE].nodeRemID)->remoteSequence++,                           // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgRead: failed serialization of packet [%d].", ret );
        return( FAILURE );
    }
    cache( getNodeFile(fh)->file.blocks[blkIndex].nodeRemID, getNodeFile(fh)->file.blocks[blkIndex].blkID, des_data_block_out);
    incLocalSeq();
    // Send the packet
    rpktlen = SG_DATA_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "sgRead: failed packet post" );
        return( FAILURE );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, des_data_block_out, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgRead: failed deserialization of packet [%d]", ret );
        return( FAILURE );
    }
    updateSeqNumber(rem, srem);
    if (localSeq != sloc)
    {
        logMessage(LOG_OUTPUT_LEVEL, "sgRead: localseq != desreialzation out");
    }
    


    //copys the asked for data
    if ( 0 == getNodeFile(fh)->file.filePos % SG_BLOCK_SIZE )
    {
        memcpy( buf, des_data_block_out, len );
        getNodeFile(fh)->file.filePos += len;    
    }
    else
    {
        //believe it works
        memcpy( buf, &des_data_block_out[SG_BLOCK_SIZE / 4], len );
        getNodeFile(fh)->file.filePos += len;
    }
    

    //Log Success
    logMessage( LOG_INFO_LEVEL, "Completed sgRead Succesfully");

    // Return the bytes processed
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgwrite
// Description  : write data to the file
//
// Inputs       : fh - file handle for the file to write to
//                buf - pointer to data to write
//                len - the length of the write
// Outputs      : number of bytes written if successful test, -1 if failure

int sgwrite(SgFHandle fh, char *buf, size_t len) {
// Local variables
    char initPacket[SG_DATA_PACKET_SIZE], recvPacket[SG_DATA_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;
    SGDataBlock des_data_block_out;

    if ( fh >= nextFile || 0 > fh ) { return FAILURE; }
    if ( getNodeFile(fh)->file.filePos > getNodeFile(fh)->file.fileSize) { logMessage(LOG_OUTPUT_LEVEL, "pos > size"); return FAILURE; }
    if ( getNodeFile(fh)->file.filePos == getNodeFile(fh)->file.fileSize )
    {
        logMessage(LOG_OUTPUT_LEVEL, "file pos == filesize");
        //setup data buffer
        char *helper_buf = (char *)calloc( SG_BLOCK_SIZE, sizeof( char ) );
        memcpy( helper_buf, buf, len );
        
        incLocalSeq();
        
        // Setup the packet
        pktlen = SG_DATA_PACKET_SIZE;
        if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                        SG_NODE_UNKNOWN,   // Remote ID
                                        SG_BLOCK_UNKNOWN,  // Block ID
                                        SG_CREATE_BLOCK,  // Operation
                                        localSeq,    // Sender sequence number
                                        SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                        helper_buf, initPacket, &pktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgWrite: failed serialization of packet [%d].", ret );
            return( FAILURE );
        }

        // Send the packet
        rpktlen = SG_DATA_PACKET_SIZE;
        if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
            logMessage( LOG_ERROR_LEVEL, "sgWrite: failed packet post" );
            return( FAILURE );
        }
        logMessage(LOG_OUTPUT_LEVEL, "after post");

        // Unpack the recieived data
        if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                        &srem, des_data_block_out, recvPacket, rpktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgWrite: failed deserialization of packet [%d]", ret );
            return( FAILURE );
        }
        logMessage(LOG_OUTPUT_LEVEL, "b4 append");
        //appendRemoteNode(rem, srem);
        updateSeqNumber(rem, srem + 1);
        
        logMessage(LOG_OUTPUT_LEVEL, "after append");

        //incRemoteSeq();

        if (localSeq != sloc)
        {
        logMessage(LOG_OUTPUT_LEVEL, "sgWrite: localseq != desreialzation out");
        }

        getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.fileSize / SG_BLOCK_SIZE].nodeRemID = rem;
        getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.fileSize / SG_BLOCK_SIZE].blkID = blkid;
        getNodeFile(fh)->file.fileSize += SG_BLOCK_SIZE;
        getNodeFile(fh)->file.filePos += len; 

        free( helper_buf);
    }
    else
    {
        logMessage( LOG_OUTPUT_LEVEL, "sgWrite where file size != filePos ");

        //setup data buffer
        char *helper_buf = (char *)calloc( SG_BLOCK_SIZE, sizeof( char ) );

        //GET THE REMOTe NODE
        
        
        //tempRemoteNode->remoteSequence++;
        incLocalSeq();
        
        // Setup the packet
        pktlen = SG_DATA_PACKET_SIZE;
        if ( (ret = serialize_sg_packet( sgLocalNodeId,                                                             // Local ID
                                        getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.filePos / SG_BLOCK_SIZE].nodeRemID,      // Remote ID
                                        getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.filePos / SG_BLOCK_SIZE].blkID,          // Block ID
                                        SG_OBTAIN_BLOCK,                                                            // Operation
                                        localSeq,                                                                   // Sender sequence number
                                        getRemoteNode(getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.filePos / SG_BLOCK_SIZE].nodeRemID)->remoteSequence++,  // Receiver sequence number
                                        NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgWrite: failed serialization of packet [%d].", ret );
            return( FAILURE );
        }
        int blkIndex = ( getNodeFile(fh)->file.filePos / (int)SG_BLOCK_SIZE );
        cache( getNodeFile(fh)->file.blocks[blkIndex].nodeRemID, getNodeFile(fh)->file.blocks[blkIndex].blkID, des_data_block_out);
        // Send the packet
        rpktlen = SG_DATA_PACKET_SIZE;
        if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
            logMessage( LOG_ERROR_LEVEL, "sgWrite: failed packet post" );
            return( FAILURE );
        }

        // Unpack the recieived data
        if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                        &srem, des_data_block_out, recvPacket, rpktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgWrite: failed deserialization of packet [%d]", ret );
            return( FAILURE );
        }
        getRemoteNode(getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.filePos / SG_BLOCK_SIZE].nodeRemID)->remoteSequence = srem;

        //remoteSeq = srem;
        incLocalSeq();
        //incRemoteSeq();

        

        logMessage(LOG_OUTPUT_LEVEL, "finished obtain");
        
        //copy data out of block
        memcpy( helper_buf, des_data_block_out, SG_BLOCK_SIZE );

        if (0 == getNodeFile(fh)->file.filePos % SG_BLOCK_SIZE )
        {
            memcpy( helper_buf, buf, len );
             
        }
        else
        {
            //believe it works
            memcpy( &helper_buf[SG_BLOCK_SIZE / 4], buf, len );
            
        }

        // Setup the packet
        pktlen = SG_DATA_PACKET_SIZE;
        if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                        getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.filePos / SG_BLOCK_SIZE].nodeRemID,   // Remote ID
                                        getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.filePos / SG_BLOCK_SIZE].blkID,  // Block ID
                                        SG_UPDATE_BLOCK,  // Operation
                                        localSeq,    // Sender sequence number
                                        (getRemoteNode(getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.filePos / SG_BLOCK_SIZE].nodeRemID)->remoteSequence++) + 1,  // Receiver sequence number
                                        helper_buf, initPacket, &pktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgWrite: failed serialization of packet [%d].", ret );
            return( FAILURE );
        }
        

        // Send the packet
        rpktlen = SG_DATA_PACKET_SIZE;
        if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
            logMessage( LOG_ERROR_LEVEL, "sgWrite: failed packet post" );
            return( FAILURE );
        }

        // Unpack the recieived data
        if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                        &srem, des_data_block_out, recvPacket, rpktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgWrite: failed deserialization of packet [%d]", ret );
            return( FAILURE );
        }
        getRemoteNode(getNodeFile(fh)->file.blocks[getNodeFile(fh)->file.filePos / SG_BLOCK_SIZE].nodeRemID)->remoteSequence = srem; 
        
        getNodeFile(fh)->file.filePos += len; 
        //free(tempRemoteNode);    
    }
    
    // Log the write, return bytes written
    logMessage( LOG_INFO_LEVEL, "Completed sgWrite Succesfully");
    
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgseek
// Description  : Seek to a specific place in the file
//
// Inputs       : fh - the file handle of the file to seek in
//                off - offset within the file to seek to
// Outputs      : new position if successful, -1 if failure

int sgseek(SgFHandle fh, size_t off) {

    logMessage(LOG_OUTPUT_LEVEL, "sgSeek: start");
    if ( fh >= nextFile || fh < 0 ) { return FAILURE; }
    if ( off > getNodeFile(fh)->file.fileSize ) { return FAILURE; }
    getNodeFile(fh)->file.filePos = off;

    //Log Success
    logMessage( LOG_INFO_LEVEL, "Completed sgSeek Succesfully");

    // Return new position
    return( off );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgclose
// Description  : Close the file
//
// Inputs       : fh - the file handle of the file to close
// Outputs      : 0 if successful test, -1 if failure

int sgclose(SgFHandle fh) 
{
    logMessage(LOG_OUTPUT_LEVEL, "sgClose: start");
    
    if ( fh >= nextFile || 0 > fh ) { return FAILURE; }
    /*struct nodeFile *temp = getNodeFile(fh); 
    
    if( temp != NULL)
    {
        //top
        if(temp->prevNode != NULL){
            temp->prevNode->nextNode = temp->nextNode;
        }
        //bottom
        if(temp->nextNode != NULL)
        {
            temp->nextNode->prevNode = temp->prevNode;
        }
        free(temp);
    }*/
   
    closeSGCache();
    //Log Success
    logMessage( LOG_INFO_LEVEL, "Completed sgClose Succesfully");

    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgshutdown
// Description  : Shut down the filesystem
//
// Inputs       : none
// Outputs      : 0 if successful test, -1 if failure

int sgshutdown(void) {

    logMessage(LOG_OUTPUT_LEVEL, "sgShutdown: start");
    // Log, return successfully
    logMessage( LOG_INFO_LEVEL, "Shut down Scatter/Gather driver." );
    return( 0 );
}

// Function     : char_pointer_is_NULL
// Description  : determines if the char* input is null
// Inputs       : char * ptr
// Outputs      : returns 0 if not, 1 if NULL
int char_pointer_is_NULL( char *ptr ) 
{
    if( NULL == ptr )
    {
        return 1;
    }   
    else
    {
        return 0;
    } 
}

// Function     : sg_node_is_good
// Description  : validate loc
// Inputs       : loc - the local node identifier 
// Outputs      : returns 0 if bad, 1 if good
int sg_node_is_good( SG_Node_ID loc )
{ 
    if( 0 == loc )
    {
        return 0;
    }   
    else
    {
        return 1;
    }} 

// Function     : remote_node_rem_validation
// Description  : validate rem
// Inputs       : rem - the remote node identifier 
// Outputs      : returns 0 if bad, 1 if good
int remote_node_is_good( SG_Node_ID rem )
{ 
    if( rem == 0)
    {
        return 0;
    }   
    else
    {
        return 1;
    }
} 

// Function     : sg_block_validation
// Description  : validate blk id
// Inputs       : blk - the block identifier
// Outputs      : returns 0 if bad, 1 if good
int sg_block_is_good( SG_Block_ID blk )
{ 
    if( blk == 0)
    {
        return 0;
    }   
    else
    {
        return 1;
    }
}
// Function     : sg_opcode_is_good
// Description  : validate op 
// Inputs       : op - the operation performed/to be performed on block
// Outputs      : returns 0 if bad, 1 if good
int sg_opcode_is_good( SG_System_OP op )
{ 
    if( op > 6 || op < 0 )
    {
        return 0;
    }
    else
    {
        return 1;
    }
} 

// Function     : sg_seq_is_good
// Description  : validate sseq
// Inputs       : sseq - the sender sequence number
// Outputs      : returns 0 if bad, 1 if good
int sg_seq_is_good( SG_SeqNum sseq )
{ 
    if( sseq == 0)
    {
        return 0;
    }   
    else
    {
        return 1;
    }
}    

// Function     : rc_seq_is_good
// Description  : validate rseq
// Inputs       : rseq - the receiver sequence number
// Outputs      : returns 0 if bad, 1 if good
int rc_seq_is_good( SG_SeqNum rseq )
{ 
    if(rseq == 0)
    { 
        return 0;
    }
    else
    {
        return 1;
    } 
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : serialize_sg_packet
// Description  : Serialize a ScatterGather packet (create packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status serialize_sg_packet(SG_Node_ID loc, SG_Node_ID rem, SG_Block_ID blk,
                                     SG_System_OP op, SG_SeqNum sseq, SG_SeqNum rseq, char *data,
                                     char *packet, size_t *plen) {

//logMessage(LOG_WARNING_LEVEL,"serialize");
    uint32_t magic_value = SG_MAGIC_VALUE; 

    //input validation 
    if( !sg_node_is_good( loc ) ) { return SG_PACKT_LOCID_BAD; }
    else if( !remote_node_is_good( rem ) ) { return SG_PACKT_REMID_BAD; }
    else if( !sg_block_is_good( blk ) ) { return SG_PACKT_BLKID_BAD; }
    else if( !sg_opcode_is_good( op ) ) { return SG_PACKT_OPERN_BAD; }
    else if( !sg_seq_is_good( sseq ) ) { return SG_PACKT_SNDSQ_BAD; }
    else if( !rc_seq_is_good( rseq )) { return SG_PACKT_RCVSQ_BAD; }
    

    //setting up necessary values 
    char data_indicator = ( char_pointer_is_NULL( data ) ) ? 0 : 1;
    
    if( data_indicator )
    {
        //building packet with data in data block


        //magic value
        memcpy(packet, &magic_value, sizeof(magic_value) ); 
        *plen = sizeof(magic_value);
        
        //Sender ID
        memcpy( ((packet) + *plen), &loc, sizeof(loc) );
        *plen += sizeof(loc);

        //Reciever ID
        memcpy( (packet) + *plen, &rem, sizeof(rem) );
        *plen += sizeof(rem);

        //Block ID
        memcpy((packet) + *plen, &blk, sizeof(blk) );
        *plen += sizeof(blk);

        //OPCODE
        memcpy((packet) + *plen, &op, sizeof(op) );
        *plen += sizeof(op);

        //Sender Sequence
        memcpy((packet) + *plen, &sseq, sizeof(sseq) );
        *plen += sizeof(sseq);

        //Reciever Sequence
        memcpy((packet) + *plen, &rseq, sizeof(rseq) );
        *plen += sizeof(rseq);

        //Data Indicator
        memcpy((packet) + *plen, &data_indicator, sizeof(data_indicator) );
        *plen += sizeof(data_indicator);

        //Data Block
        memcpy((packet) + *plen, data, SG_BLOCK_SIZE );
        *plen += SG_BLOCK_SIZE;

        //magic value
        memcpy(packet, &magic_value, sizeof(magic_value) ); 
        *plen += sizeof(magic_value);
        
        //logMessage(LOG_WARNING_LEVEL,"Serialize, Line 215, Full Data, returns ok");
        return SG_PACKT_OK;
        
    }
    else 
    {
        //building packet with no data in data block

        //magic value
        memcpy(packet, &magic_value, sizeof(magic_value) ); 
        *plen = sizeof(magic_value);
        
        //Sender ID
        memcpy( ((packet) + *plen), &loc, sizeof(loc) );
        *plen += sizeof(loc);

        //Reciever ID
        memcpy((packet) + *plen, &rem, sizeof(rem) );
        *plen += sizeof(rem);

        //Block ID
        memcpy((packet) + *plen, &blk, sizeof(blk) );
        *plen += sizeof(blk);

        //OPCODE
        memcpy((packet) + *plen, &op, sizeof(op) );
        *plen += sizeof(op);

        //Sender Sequence
        memcpy((packet) + *plen, &sseq, sizeof(sseq) );
        *plen += sizeof(sseq);

        //Reciever Sequence
        memcpy((packet) + *plen, &rseq, sizeof(rseq) );
        *plen += sizeof(rseq);

        //Data Indicator
        memcpy((packet) + *plen, &data_indicator, sizeof(data_indicator) );
        *plen += sizeof(data_indicator);

        //magic value
        memcpy(packet, &magic_value, sizeof(magic_value) ); 
        *plen += sizeof(magic_value);

        return SG_PACKT_OK;
    }
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : deserialize_sg_packet
// Description  : De-serialize a ScatterGather packet (unpack packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status deserialize_sg_packet(SG_Node_ID *loc, SG_Node_ID *rem, SG_Block_ID *blk,
                                       SG_System_OP *op, SG_SeqNum *sseq, SG_SeqNum *rseq, char *data,
                                       char *packet, size_t plen) {

    uint32_t *magic_value = malloc( sizeof(uint32_t) );
    //char x[256];
    //sprintf(x, "%ld", plen);
    
    char data_indicator;
    //logMessage(LOG_WARNING_LEVEL, "before plen"); 
    //logMessage( LOG_WARNING_LEVEL, x);
    //logMessage( LOG_WARNING_LEVEL,"Afterplen");

    int ptr;
    //logMessage( LOG_WARNING_LEVEL,  plen);
    

    if( plen > SG_BASE_PACKET_SIZE )
    {

        //parsing packet with data in block

        //logMessage(LOG_WARNING_LEVEL, "DEserialize, Line 313, data in block");

        //magic value
        memcpy( magic_value, packet, sizeof(*magic_value) ); 
        ptr = sizeof(*magic_value);
        
        //Sender ID
        memcpy( loc, ((packet) + ptr), sizeof(*loc) );
        ptr += sizeof(*loc);

        //Reciever ID
        memcpy( rem, (packet) + ptr, sizeof(*rem) );
        ptr += sizeof(*rem);

        //Block ID
        memcpy( blk, (packet) + ptr, sizeof(*blk) );
        ptr += sizeof(*blk);

        //OPCODE
        memcpy( op, (packet) + ptr, sizeof(*op) );
        ptr += sizeof(*op);

        //Sender Sequence
        memcpy( sseq, (packet) + ptr, sizeof(*sseq) );
        ptr += sizeof(*sseq);

        //Reciever Sequence
        memcpy( rseq, (packet) + ptr, sizeof(*rseq) );
        ptr += sizeof(*rseq);

        //Data Indicator
        memcpy( &data_indicator, (packet) + ptr, sizeof(data_indicator) );
        ptr += sizeof(data_indicator);

        //Data Block
        memcpy( data, (packet) + ptr, SG_BLOCK_SIZE );
        ptr += SG_BLOCK_SIZE;

        //magic value
        memcpy( magic_value, packet + ptr, sizeof(*magic_value) ); 
        ptr += sizeof(*magic_value);
        //logMessage(LOG_WARNING_LEVEL, "DEserialize, Line 353, data in block, end of getting data");
    }
    else 
    {
        //parsing packet with no data in data block

        //logMessage(LOG_WARNING_LEVEL, "DEserialize, Line 358, no data in block");
        //magic value
        memcpy( magic_value, packet, sizeof(*magic_value) ); 
        ptr = sizeof(*magic_value);
        
        //Sender ID
        memcpy( loc, ((packet) + ptr), sizeof(*loc) );
        ptr += sizeof(*loc);

        //Reciever ID
        memcpy( rem, (packet) + ptr, sizeof(*rem) );
        ptr += sizeof(*rem);

        //Block ID
        memcpy( blk, (packet) + ptr, sizeof(*blk) );
        ptr += sizeof(*blk);

        //OPCODE
        memcpy( op, (packet) + ptr, sizeof(*op) );
        ptr += sizeof(*op);

        //Sender Sequence
        memcpy( sseq, (packet) + ptr, sizeof(*sseq) );
        ptr += sizeof(*sseq);

        //Reciever Sequence
        memcpy( rseq, (packet) + ptr, sizeof(*rseq) );
        ptr += sizeof(*rseq);

        //Data Indicator
        memcpy( &data_indicator, (packet) + ptr, sizeof(data_indicator) );
        ptr += sizeof(data_indicator);

        //magic value
        memcpy( magic_value, packet + ptr, sizeof(*magic_value) ); 
        ptr += sizeof(*magic_value);
        //logMessage(LOG_WARNING_LEVEL, "DEserialize, Line 394, no data in block, end of getting data");

    }
    //logMessage(LOG_WARNING_LEVEL,"starting deserializtion return values");

    //validation of packet data
    if( !sg_node_is_good( *loc ) ) {  return SG_PACKT_LOCID_BAD; }
    if( !remote_node_is_good( *rem ) ) { return SG_PACKT_REMID_BAD; }
    if( !sg_block_is_good( *blk ) ) { return SG_PACKT_BLKID_BAD; }
    if( !sg_opcode_is_good( *op ) ) { return SG_PACKT_OPERN_BAD; }
    if( !sg_seq_is_good( *sseq ) ) { return SG_PACKT_SNDSQ_BAD; }
    if( !rc_seq_is_good( *rseq )) { return SG_PACKT_RCVSQ_BAD; }

    //logMessage(LOG_WARNING_LEVEL, "returning deserialization ok"); 

    return SG_PACKT_OK;
}

//
// Driver support functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgInitEndpoint
// Description  : Initialize the endpoint
//
// Inputs       : none
// Outputs      : 0 if successfull, -1 if failure

int sgInitEndpoint( void ) {

    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    // Local and do some initial setup
    logMessage( LOG_INFO_LEVEL, "Initializing local endpoint ..." );
    sgLocalSeqno = SG_INITIAL_SEQNO;

    // Setup the packet
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( SG_NODE_UNKNOWN, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_INIT_ENDPOINT,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret );
        return( -1 );
    }

    incLocalSeq();
    
    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    // Sanity check the return value
    if ( loc == SG_NODE_UNKNOWN ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", loc );
        return( -1 );
    }

    // Set the local node ID, log and return successfully
    sgLocalNodeId = loc;
    logMessage( LOG_INFO_LEVEL, "Completed initialization of node (local node ID %lu", sgLocalNodeId );
    return( 0 );
}