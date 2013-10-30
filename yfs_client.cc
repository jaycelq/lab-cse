// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define NAME_MAX 128

struct dir_ent{
    char filename[NAME_MAX];
    yfs_client::inum inum;
};

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

#define min(x, y) ((x) < (y) ? (x) : (y))
#define max(x, y) ((x) > (y) ? (x) : (y))

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    char buffer[size];
    const char* data;
    std::string content;
    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    memset(buffer, 0, size);
    if(isfile(ino)){
        ec->get(ino, content);
        data = content.data();
        memcpy(buffer, data, min(size, content.size()));
        content.assign(buffer, min(size, content.size()));
        ec->put(ino, content);
    }
    else r = IOERR;
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out, extent_protocol::types type)
{
    int r = OK;
    bool found = false;
    struct dir_ent* buffer;
    std::string dir_content;
    const char *content_org;
    char *content_append;

    buffer = (struct dir_ent *)malloc(sizeof(struct dir_ent));
    memset(buffer, 0, sizeof(struct dir_ent));
    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    lookup(parent, name, found, ino_out);
    if(!found){
        ec->get(parent, dir_content);
        ec->create(type, ino_out);
        strncpy(buffer->filename, name, NAME_MAX);
        buffer->inum = ino_out;
        content_org = dir_content.data();
        content_append = (char *)malloc(dir_content.size() + sizeof(struct dir_ent));
        memcpy(content_append, content_org, dir_content.size());
        memcpy(content_append + dir_content.size(), buffer, sizeof(struct dir_ent));
        dir_content.assign(content_append, dir_content.size() + sizeof(struct dir_ent)); 
        ec->put(parent, dir_content);
        free(content_append);
    }
    else r = EXIST;

    free(buffer);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    uint32_t offset;
    std::string dir_content;
    struct dir_ent* buffer;
    const char* content;

    found = false;
    ino_out = 0;
    buffer = (struct dir_ent *)malloc(sizeof(struct dir_ent));
    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    /* 
     * file name is fixed-length of 128 bytes
     * dir structure <filename[], inum, filename[], inum>
     * temporarily ignore return value for caller just focus on found
     */
    if(isdir(parent)){
        ec->get(parent, dir_content);
        content = dir_content.data(); 
        for(offset = 0; offset < dir_content.size(); offset += sizeof(struct dir_ent)){
            memcpy((char *)buffer,content+offset,sizeof(struct dir_ent));
            if(strncmp(buffer->filename, name, NAME_MAX) == 0){
                found = true;
                ino_out = buffer-> inum;
                break;
            }
        }
        
    }    
    free(buffer);
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    uint32_t offset;
    std::string dir_content;
    struct dir_ent* buffer;
    struct dirent entry;
    const char* content;
    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    buffer = (struct dir_ent *)malloc(sizeof(struct dir_ent));

    if(isdir(dir)){
        ec->get(dir, dir_content);
        content = dir_content.data();
        for(offset = 0; offset < dir_content.size(); offset += sizeof(struct dir_ent)){
            memcpy((char *)buffer,content+offset,sizeof(struct dir_ent));
            entry.name.assign(buffer->filename, strlen(buffer->filename));
            entry.inum = buffer->inum;
            list.push_back(entry);
        }

    }
    else r = NOENT;
    free(buffer);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    std::string content;
    const char* content_org;
    char buffer[size];
    
    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */
    memset(buffer, 0, size);
    if(isfile(ino)){
        ec->get(ino, content);
        content_org = content.data(); 
        memcpy(buffer, content_org + off, min(size, content.size() - off));
        data.assign(buffer, min(size, content.size() - off));
    }
    else r = IOERR;

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    std::string content;
    const char* content_org;
    char* content_aft;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    if(isfile(ino)){
        ec->get(ino, content);
        content_org = content.data();
        content_aft = (char *)malloc(max(off + size, content.size()));
        bzero(content_aft, max(off + size, content.size()));
        memcpy(content_aft, content_org, min(off, content.size()));
        memcpy(content_aft + off, data, size);
        if(content.size() > (off + size)){
            memcpy(content_aft + off + size, content_org + off + size, content.size() - off -size);
        }
        content.assign(content_aft, max(off + size, content.size()));
        ec->put(ino, content);
        bytes_written = size;
        free(content_aft); 
    }
    else r = IOERR;
    
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = NOENT;
    uint32_t offset;
    char* content_deleted;
    const char* content;
    struct dir_ent* buffer;
    std::string dir_content;
    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    buffer = (struct dir_ent *)malloc(sizeof(struct dir_ent));
    if(isdir(parent)){
        ec->get(parent, dir_content);
        content = dir_content.data();
        for(offset = 0; offset < dir_content.size(); offset += sizeof(struct dir_ent)){
            memcpy((char*) buffer, content + offset, sizeof(struct dir_ent));
            if(strncmp(buffer->filename, name, NAME_MAX) == 0){
                ec->remove(buffer->inum);
               	content_deleted = (char *)malloc(dir_content.size() - sizeof(dir_ent));
                memcpy(content_deleted, content, offset);
                memcpy(content_deleted + offset, content + offset + sizeof(dir_ent), dir_content.size() - offset - sizeof(struct dir_ent)); 
                dir_content.assign(content_deleted, dir_content.size() - sizeof(dir_ent));
                ec->put(parent, dir_content);
                free(content_deleted);
                r = OK;
                break;
            }
        }
    }
    free(buffer);
    return r;
}

