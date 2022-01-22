package client

import com.google.common.base.Strings
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import seaweedfs.client.FilerClient
import seaweedfs.client.FilerGrpcClient
import seaweedfs.client.FilerProto.*
import seaweedfs.client.SeaweedFilerGrpcKt
import java.io.File
import java.util.*

class FilerClientKt(host: String?, grpcPort: Int) :
    FilerGrpcClient(host, grpcPort - 10_000, grpcPort) {
    suspend fun mkdirs(path: String, mode: Int): Boolean {
        val currentUser = System.getProperty("user.name")
        return mkdirs(path, mode, 0, 0, currentUser, arrayOf())
    }

    val channel = ManagedChannelBuilder.forAddress("box.lan", 17777).usePlaintext()
        .maxInboundMessageSize(1024 * 1024 * 1024).build()
    val stub = SeaweedFilerGrpcKt.SeaweedFilerCoroutineStub(channel)

    suspend fun mkdirs(path: String, mode: Int, userName: String?, groupNames: Array<String?>): Boolean {
        return mkdirs(path, mode, 0, 0, userName, groupNames)
    }

    suspend fun mkdirs(
        path: String,
        mode: Int,
        uid: Int,
        gid: Int,
        userName: String?,
        groupNames: Array<String?>
    ): Boolean {
        if ("/" == path) {
            return true
        }
        val pathFile = File(path)
        val parent = pathFile.parent.replace('\\', '/')
        val name = pathFile.name
        mkdirs(parent, mode, uid, gid, userName, groupNames)
        val existingEntry = lookupEntry(parent, name)
        return if (existingEntry != null) {
            true
        } else createEntry(
            parent,
            newDirectoryEntry(name, mode, uid, gid, userName, groupNames).build()
        )
    }

    suspend fun mv(oldPath: String, newPath: String): Boolean {
        val oldPathFile = File(oldPath)
        val oldParent = oldPathFile.parent.replace('\\', '/')
        val oldName = oldPathFile.name
        val newPathFile = File(newPath)
        val newParent = newPathFile.parent.replace('\\', '/')
        val newName = newPathFile.name
        return atomicRenameEntry(oldParent, oldName, newParent, newName)
    }

    suspend fun exists(path: String): Boolean {
        val pathFile = File(path)
        var parent = pathFile.parent
        var entryName = pathFile.name
        if (parent == null) {
            parent = path
            entryName = ""
        }
        return lookupEntry(parent, entryName) != null
    }

    suspend fun rm(path: String, isRecursive: Boolean, ignoreRecusiveError: Boolean): Boolean {
        val pathFile = File(path)
        val parent = pathFile.parent.replace('\\', '/')
        val name = pathFile.name
        return deleteEntry(
            parent,
            name,
            true,
            isRecursive,
            ignoreRecusiveError
        )
    }

    suspend fun touch(path: String, mode: Int): Boolean {
        val currentUser = System.getProperty("user.name")
        val now = System.currentTimeMillis() / 1000L
        return touch(path, now, mode, 0, 0, currentUser, arrayOf())
    }

    suspend fun touch(
        path: String,
        modifiedTimeSecond: Long,
        mode: Int,
        uid: Int,
        gid: Int,
        userName: String,
        groupNames: Array<String>
    ): Boolean {
        val pathFile = File(path)
        val parent = pathFile.parent.replace('\\', '/')
        val name = pathFile.name
        val entry = lookupEntry(parent, name)
            ?: return createEntry(
                parent,
                newFileEntry(name, modifiedTimeSecond, mode, uid, gid, userName, groupNames).build()
            )
        val attr = entry.attributes.toBuilder()
        if (modifiedTimeSecond > 0) {
            attr.mtime = modifiedTimeSecond
        }
        if (uid > 0) {
            attr.uid = uid
        }
        if (gid > 0) {
            attr.gid = gid
        }
        attr.userName = userName
        attr.clearGroupName().addAllGroupName(listOf(*groupNames))
        return updateEntry(parent, entry.toBuilder().setAttributes(attr).build())
    }

    fun newDirectoryEntry(
        name: String?, mode: Int,
        uid: Int, gid: Int, userName: String?, groupNames: Array<String?>
    ): Entry.Builder {
        val now = System.currentTimeMillis() / 1000L
        return Entry.newBuilder()
            .setName(name)
            .setIsDirectory(true)
            .setAttributes(
                FuseAttributes.newBuilder()
                    .setMtime(now)
                    .setCrtime(now)
                    .setUid(uid)
                    .setGid(gid)
                    .setFileMode(mode or 1 shl 31)
                    .setUserName(userName)
                    .clearGroupName()
                    .addAllGroupName(listOf(*groupNames))
            )
    }

    fun newFileEntry(
        name: String?, modifiedTimeSecond: Long, mode: Int,
        uid: Int, gid: Int, userName: String?, groupNames: Array<String>
    ): Entry.Builder {
        return Entry.newBuilder()
            .setName(name)
            .setIsDirectory(false)
            .setAttributes(
                FuseAttributes.newBuilder()
                    .setMtime(modifiedTimeSecond)
                    .setCrtime(modifiedTimeSecond)
                    .setUid(uid)
                    .setGid(gid)
                    .setFileMode(mode)
                    .setUserName(userName)
                    .clearGroupName()
                    .addAllGroupName(listOf(*groupNames))
            )
    }

    suspend fun listEntries(path: String?): List<Entry> {
        val results: MutableList<Entry> = ArrayList()
        var lastFileName = ""
        var limit = Int.MAX_VALUE
        while (limit > 0) {
            val t = listEntries(path, "", lastFileName, 1024, false) ?: break
            val nSize = t.size
            if (nSize > 0) {
                limit -= nSize
                lastFileName = t[nSize - 1].name
            }
            results.addAll(t)
            if (t.size < 1024) {
                break
            }
        }
        return results
    }

    suspend fun listEntries(
        path: String?,
        entryPrefix: String?,
        lastEntryName: String?,
        limit: Int,
        includeLastEntry: Boolean
    ): List<Entry> {
        val iter = stub.listEntries(
            ListEntriesRequest.newBuilder()
                .setDirectory(path)
                .setPrefix(entryPrefix)
                .setStartFromFileName(lastEntryName)
                .setInclusiveStartFrom(includeLastEntry)
                .setLimit(limit)
                .build()
        )

        return iter.map { afterEntryDeserialization(it.entry) }.toList()
    }

    suspend fun lookupEntry(directory: String?, entryName: String?): Entry? {
        return try {
            val entry: Entry = stub.lookupDirectoryEntry(
                LookupDirectoryEntryRequest.newBuilder()
                    .setDirectory(directory)
                    .setName(entryName)
                    .build()
            ).getEntry() ?: return null
            afterEntryDeserialization(entry)
        } catch (e: Exception) {
            if (e.message!!.indexOf("filer: no entry is found in filer store") > 0) {
                return null
            }
            LOG.warn("lookupEntry {}/{}: {}", directory, entryName, e)
            null
        }
    }

    suspend fun createEntry(parent: String?, entry: Entry): Boolean {
        return try {
            val createEntryResponse: CreateEntryResponse = stub.createEntry(
                CreateEntryRequest.newBuilder()
                    .setDirectory(parent)
                    .setEntry(entry)
                    .build()
            )
            if (Strings.isNullOrEmpty(createEntryResponse.error)) {
                return true
            }
            LOG.warn("createEntry {}/{} error: {}", parent, entry.name, createEntryResponse.error)
            false
        } catch (e: Exception) {
            LOG.warn("createEntry {}/{}: {}", parent, entry.name, e)
            false
        }
    }

    suspend fun updateEntry(parent: String?, entry: Entry): Boolean {
        try {
            stub.updateEntry(
                UpdateEntryRequest.newBuilder()
                    .setDirectory(parent)
                    .setEntry(entry)
                    .build()
            )
        } catch (e: Exception) {
            LOG.warn("updateEntry {}/{}: {}", parent, entry.name, e)
            return false
        }
        return true
    }

    suspend fun deleteEntry(
        parent: String?,
        entryName: String?,
        isDeleteFileChunk: Boolean,
        isRecursive: Boolean,
        ignoreRecusiveError: Boolean
    ): Boolean {
        try {
            stub.deleteEntry(
                DeleteEntryRequest.newBuilder()
                    .setDirectory(parent)
                    .setName(entryName)
                    .setIsDeleteData(isDeleteFileChunk)
                    .setIsRecursive(isRecursive)
                    .setIgnoreRecursiveError(ignoreRecusiveError)
                    .build()
            )
        } catch (e: Exception) {
            LOG.warn("deleteEntry {}/{}: {}", parent, entryName, e)
            return false
        }
        return true
    }

    suspend fun atomicRenameEntry(oldParent: String?, oldName: String?, newParent: String?, newName: String?): Boolean {
        try {
            stub.atomicRenameEntry(
                AtomicRenameEntryRequest.newBuilder()
                    .setOldDirectory(oldParent)
                    .setOldName(oldName)
                    .setNewDirectory(newParent)
                    .setNewName(newName)
                    .build()
            )
        } catch (e: Exception) {
            LOG.warn("atomicRenameEntry {}/{} => {}/{}: {}", oldParent, oldName, newParent, newName, e)
            return false
        }
        return true
    }

    fun watch(prefix: String?, clientName: String?, sinceNs: Long): Flow<SubscribeMetadataResponse> {
        return stub.subscribeMetadata(
            SubscribeMetadataRequest.newBuilder()
                .setPathPrefix(prefix)
                .setClientName(clientName)
                .setSinceNs(sinceNs)
                .build()
        )
    }

//    fun watch2(prefix: String?, clientName: String?, sinceNs: Long): SendChannel<SubscribeMetadataResponse> {
//        val flow = stub.subscribeMetadata(
//            SubscribeMetadataRequest.newBuilder()
//                .setPathPrefix(prefix)
//                .setClientName(clientName)
//                .setSinceNs(sinceNs)
//                .build()
//        )
//
//        val actor = GlobalScope.actor<SubscribeMetadataResponse> {
//            for (msg in channel)
//                runCatching {
//                    println(msg)
//                }
//        }
//        val out = flow.produceIn(GlobalScope)
//
//        out.pipeTo(actor)
//
//        return actor
//    }

    companion object {
        private val LOG = LoggerFactory.getLogger(FilerClient::class.java)
        fun toFileId(fid: FileId?): String? {
            return if (fid == null) {
                null
            } else String.format("%d,%x%08x", fid.volumeId, fid.fileKey, fid.cookie)
        }

        fun toFileIdObject(fileIdStr: String?): FileId? {
            if (fileIdStr == null || fileIdStr.length == 0) {
                return null
            }
            val commaIndex = fileIdStr.lastIndexOf(',')
            val volumeIdStr = fileIdStr.substring(0, commaIndex)
            val fileKeyStr = fileIdStr.substring(commaIndex + 1, fileIdStr.length - 8)
            val cookieStr = fileIdStr.substring(fileIdStr.length - 8)
            return FileId.newBuilder()
                .setVolumeId(volumeIdStr.toInt())
                .setFileKey(fileKeyStr.toLong(16))
                .setCookie(cookieStr.toLong(16).toInt())
                .build()
        }

        fun beforeEntrySerialization(chunks: List<FileChunk>): List<FileChunk> {
            val cleanedChunks: MutableList<FileChunk> = ArrayList()
            for (chunk in chunks) {
                val chunkBuilder = chunk.toBuilder()
                chunkBuilder.clearFileId()
                chunkBuilder.clearSourceFileId()
                chunkBuilder.fid = toFileIdObject(chunk.fileId)
                val sourceFid = toFileIdObject(chunk.sourceFileId)
                if (sourceFid != null) {
                    chunkBuilder.sourceFid = sourceFid
                }
                cleanedChunks.add(chunkBuilder.build())
            }
            return cleanedChunks
        }

        fun afterEntryDeserialization(entry: Entry): Entry {
            if (entry.chunksList.size <= 0) {
                return entry
            }
            val fileId = entry.getChunks(0).fileId
            if (fileId != null && fileId.length != 0) {
                return entry
            }
            val entryBuilder = entry.toBuilder()
            entryBuilder.clearChunks()
            for (chunk in entry.chunksList) {
                val chunkBuilder = chunk.toBuilder()
                chunkBuilder.fileId = toFileId(chunk.fid)
                val sourceFileId = toFileId(chunk.sourceFid)
                if (sourceFileId != null) {
                    chunkBuilder.sourceFileId = sourceFileId
                }
                entryBuilder.addChunks(chunkBuilder)
            }
            return entryBuilder.build()
        }
    }
}