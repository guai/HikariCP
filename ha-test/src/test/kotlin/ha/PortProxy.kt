package ha

import java.io.Closeable
import java.io.IOException
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import kotlin.concurrent.thread


class PortProxy(private val host: String, private val remotePort: Int, private val localPort: Int) : Thread(), AutoCloseable {
    private val sockets = ArrayList<Closeable>()

    init {
        name = "Proxy main thread host = $host, remotePort = $remotePort, localPort = $localPort"
        isDaemon = true
    }

    override fun run() {
        val listeningSocket = ServerSocket(localPort).also {
            sockets.add(it)
        }

        while (!listeningSocket.isClosed) {
            val (fromClient, toClient) = listeningSocket.accept().let {
                sockets.add(it)
                it.getInputStream() to it.getOutputStream()
            }

            val (fromServer, toServer) = Socket(host, remotePort).let {
                sockets.add(it)
                it.getInputStream() to it.getOutputStream()
            }

            thread(name = "Proxy worker", isDaemon = true, start = true) {
                val buffer = ByteArray(4096)
                try {
                    while (true) {
                        val bytesRead = fromClient.read(buffer)
                        if (bytesRead == -1) break
                        toServer.write(buffer, 0, bytesRead)
                        toServer.flush()
                    }
                } catch (e: IOException) {
                }
            }

            thread(name = "Proxy worker", isDaemon = true, start = true) {
                val buffer = ByteArray(4096)
                try {
                    while (true) {
                        val bytesRead = fromServer.read(buffer)
                        if (bytesRead == -1) break
                        toClient.write(buffer, 0, bytesRead)
                        toClient.flush()
                    }
                } catch (e: IOException) {
                }
            }
        }
    }

    override fun close() {
        for (socket in sockets)
            try {
                socket.close()
            } catch (e: IOException) {
            }
        sockets.clear()
    }
}
