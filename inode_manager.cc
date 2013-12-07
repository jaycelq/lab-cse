#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  uint32_t b, bit;
  char buf[BLOCK_SIZE], bit_mask;
 
  memset(buf, 0, BLOCK_SIZE);
  pthread_mutex_lock(&block_lock);
  for(b = 0; b < sb.nblocks; b += BPB){
    read_block(BBLOCK(b), buf);
    for(bit = 0; bit < BPB && bit < sb.nblocks - b; bit++){
      bit_mask = 1 << (bit % 8);
      // the block is free
      if((buf[bit/8] & bit_mask) == 0){
        buf[bit/8] |= bit_mask;
        write_block(BBLOCK(b), buf);
        printf("block_layer %d\n", b+bit);
        pthread_mutex_unlock(&block_lock);
        return b + bit;
      }
    } 
  }
  pthread_mutex_unlock(&block_lock);
  // no free block found
  printf("Blocks are using out\n");
  exit(0);
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  char buf[BLOCK_SIZE];
  uint32_t bit, mask;

  // Protect superblock bitmap block and inode blocks
  if(id < RSVD_BLOCKS(sb.nblocks)) {
    printf("Reserved blocks can't be freed\n");
    exit(0);
  }
  pthread_mutex_lock(&block_lock);
  memset(buf, 0, BLOCK_SIZE);

  read_block(BBLOCK(id), buf);  
  
  bit = id % BPB;
  mask = 1 << (bit % 8);
  // test whether the block has been freed.
  if((buf[bit/8] & mask) == 0){
    printf("block has been freed\n");
  }
  buf[bit/8] &= ~mask;
  
  write_block(BBLOCK(id), buf);
  
  // reset the block when free it
  bzero(buf, BLOCK_SIZE);
  write_block(id, buf);
  pthread_mutex_unlock(&block_lock);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  uint32_t i;
  char buf[BLOCK_SIZE];
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  pthread_mutex_init(&block_lock, NULL);

  // alloc the unused block, super block and inode blocks
  for(i = 0; i < RSVD_BLOCKS(sb.nblocks); i++)  alloc_block();       
  
  // write superblock to disk
  memset(buf, 0, BLOCK_SIZE);
  memcpy(buf, &sb, sizeof(sb));
  // super block is at block 1
  write_block(1, buf);
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  inode_start = 1;
  bm = new block_manager();
  pthread_mutex_init(&alloc_lock, NULL);
  pthread_mutex_init(&inode_lock, NULL);
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  uint32_t inode_num, ino_num;
  char buf[BLOCK_SIZE];
  inode_t *inode;
  
  // use a mutex to prevent two threads alloc a same inode
  pthread_mutex_lock(&alloc_lock);
  // read the inode one by one, if free, alloc it by setting inode type
   
  for(inode_num = inode_start; inode_num < bm->sb.ninodes + inode_start; inode_num++){
    ino_num = inode_num % bm->sb.ninodes;
    if(ino_num == 0) ino_num++;
    bm->read_block(IBLOCK(ino_num, bm->sb.nblocks), buf);
    inode = (inode_t *) buf + ino_num % IPB;
    if(inode->type == 0){
      inode->type = type;
      inode->ctime = inode->mtime = time(NULL);
      bm->write_block(IBLOCK(ino_num, bm->sb.nblocks), buf);
      inode_start = ino_num;
      pthread_mutex_unlock(&alloc_lock);
      return ino_num;
    }
  }
  
  printf("inode blocks are full\n");
  pthread_mutex_unlock(&alloc_lock);
  exit(0);
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t *inode;

  pthread_mutex_lock(&alloc_lock);
  // free the inum inode by resetting inode->type
  inode = get_inode(inum);
  if(inode->type == 0){
    printf("freeing free inode, not allowed\n");
    exit(0);
  }

  memset(inode, 0, sizeof(inode_t));
  put_inode(inum, inode);
  free(inode);
  pthread_mutex_unlock(&alloc_lock);

  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];
  pthread_mutex_lock(&inode_lock);
  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    pthread_mutex_unlock(&inode_lock);
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;
  pthread_mutex_unlock(&inode_lock);
  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;
  
  pthread_mutex_lock(&inode_lock);
  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  pthread_mutex_unlock(&inode_lock);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  inode_t *inode;
  uint32_t block_num, len, offset;
  uint32_t *indirect;
  char rbuf[BLOCK_SIZE], ind_buf[BLOCK_SIZE];
  char *buffer;
  printf("read file \n");
  inode = get_inode(inum);
  *size = inode->size;

  *buf_out = (char *)malloc(inode->size);
  buffer = *buf_out;
  memset(buffer, 0, inode->size);

  // determine whether need to use indirect blocks
  if(inode->size/BLOCK_SIZE >= NDIRECT){
    bm->read_block(inode->blocks[NDIRECT], ind_buf);
    indirect = (uint32_t *)ind_buf;
  }

  for(len = 0; len < inode->size; len += offset, buffer += offset){
    offset = MIN(inode->size - len, BLOCK_SIZE);
    block_num = len/BLOCK_SIZE;
    if(block_num < NDIRECT){
      // read the direct block
      bm->read_block(inode->blocks[block_num], rbuf);
      memcpy(buffer, rbuf, offset);
    }
    else{
      // read the indirect block
      block_num -= NDIRECT;
      bm->read_block(indirect[block_num], rbuf);
      memcpy(buffer, rbuf, offset);
    }
    memset(rbuf, 0, BLOCK_SIZE);
  }
  inode->atime = time(NULL);
  put_inode(inum,inode);
  free(inode);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  int block_num = -1, block_id = 0;
  uint32_t *indirect = NULL;
  int len = 0, offset = 0;
  inode_t *inode;
  char wbuf[BLOCK_SIZE], rbuf[BLOCK_SIZE];
  
  inode = get_inode(inum);
  //printf("inode_layer:write size %d \n", size);
  // if write buffer is bigger than the max_size of a file ,just write the heading max_size bits.
  inode->size = MIN((uint32_t) size, MAXFILE * BLOCK_SIZE);
  put_inode(inum, inode);

  // determine whether need indirect block
  if(size/BLOCK_SIZE > NDIRECT){
    if(inode->blocks[NDIRECT] == 0) inode->blocks[NDIRECT] = bm->alloc_block();
    //printf("inode_layer:NDIRECT %d\n", inode->blocks[NDIRECT]);
    bm->read_block(inode->blocks[NDIRECT], rbuf);
    indirect = (uint32_t *)rbuf;
  } 
 
  for(len = 0; len < size; len += offset, buf += offset){
    offset = MIN(size - len, BLOCK_SIZE);
    block_num = len/BLOCK_SIZE;
    if(block_num < NDIRECT){
      // write direct block
      if(inode->blocks[block_num] == 0) inode->blocks[block_num] = bm->alloc_block();
      block_id = inode->blocks[block_num];
      //printf("inode_layer:block_num %d blockid %d\n", block_num, block_id);
    }
    else{
      // write indirect block
      if(indirect[block_num - NDIRECT] == 0) {
        indirect[block_num - NDIRECT] = bm->alloc_block();
        bm->write_block(inode->blocks[NDIRECT], rbuf);
      } 
      block_id = indirect[block_num - NDIRECT];
      //printf("inode_layer:INDIRECT block_num %d blockid %d\n", block_num - NDIRECT, block_id);
    }
    memset(wbuf, 0, BLOCK_SIZE);
    memcpy(wbuf, buf, offset);
    bm->write_block(block_id, wbuf);
  }

  // if the original file is bigger, free the rest
  block_num++;
  for(;block_num < (int) MAXFILE; block_num++){
    if(block_num < NDIRECT){
      if(inode->blocks[block_num] == 0)  break;
      bm->free_block(inode->blocks[block_num]);
      inode->blocks[block_num] = 0;
    }
    else{
      if(block_num == NDIRECT) {
        if(inode->blocks[NDIRECT] == 0) break;
        bm->read_block(inode->blocks[NDIRECT], rbuf);
        indirect = (uint32_t *)rbuf;
        bm->free_block(inode->blocks[NDIRECT]);
        inode->blocks[NDIRECT] = 0;
      }
      if(indirect[block_num - NDIRECT] == 0) break;
      bm->free_block(indirect[block_num - NDIRECT]);
      indirect[block_num - NDIRECT] = 0;
    }
  } 
  inode->ctime = inode->mtime = time(NULL);
  put_inode(inum, inode);
  free(inode);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */

  inode_t *inode;
  inode = get_inode(inum); 
  if(inode != NULL) {
    a.type = inode->type;
	a.size = inode->size;
	a.atime = inode->atime;
	a.ctime = inode->ctime;
	a.mtime = inode->mtime;
  }
  else a.type = 0;
  free(inode);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  
  inode_t *inode;
  uint32_t block_num, len, offset;
  char ind_buf[BLOCK_SIZE];
  uint32_t *indirect;
  
  inode = get_inode(inum);

  //printf("remove file size %d\n", inode->size);
  //printf("inode_layer:NDIRECT %d\n", inode->blocks[NDIRECT]);
  if(inode->size/BLOCK_SIZE > NDIRECT){
    bm->read_block(inode->blocks[NDIRECT], ind_buf);
    bm->free_block(inode->blocks[NDIRECT]);
    inode->blocks[NDIRECT] = 0;
    indirect = (uint32_t *)ind_buf;
  }

  for(len = 0; len < inode->size; len += offset){
    offset = MIN(inode->size - len, BLOCK_SIZE);
    block_num = len/BLOCK_SIZE;
    if(block_num < NDIRECT) {
      bm->free_block(inode->blocks[block_num]);
      //printf("inode_layer block_num %d blockid %d\n", block_num,  inode->blocks[block_num]);
      inode->blocks[block_num] = 0;
    }
    else{
      block_num -= NDIRECT;
      bm->free_block(indirect[block_num]);
      //printf("inode_layer block_num %d blockid %d\n", block_num+NDIRECT,  indirect[block_num]);
      indirect[block_num] = 0;
    }
  }
  free_inode(inum);
  return;
}
