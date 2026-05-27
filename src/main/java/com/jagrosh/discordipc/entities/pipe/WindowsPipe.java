package com.jagrosh.discordipc.entities.pipe;

import com.jagrosh.discordipc.IPCClient;
import com.jagrosh.discordipc.entities.Callback;
import com.jagrosh.discordipc.entities.Packet;

import org.json.JSONException;
import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.util.HashMap;

public class WindowsPipe extends Pipe
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(WindowsPipe.class);

    private final RandomAccessFile file;

    WindowsPipe(
            IPCClient ipcClient,
            HashMap<String, Callback> callbacks,
            String location
    ) {
        super(ipcClient, callbacks);

        try
        {
            this.file = new RandomAccessFile(
                    location,
                    "rw"
            );
        }
        catch(FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        if(status == PipeStatus.CLOSED)
        {
            throw new IOException(
                    "Pipe is closed!"
            );
        }

        file.write(b);

        file.getFD().sync();
    }

    @Override
    public Packet read() throws IOException, JSONException
    {
        while(
                file.length() == 0
                        && status == PipeStatus.CONNECTED
                        && !Thread.currentThread().isInterrupted()
        ) {
            try
            {
                Thread.sleep(50);
            }
            catch(InterruptedException e)
            {
                Thread.currentThread().interrupt();

                throw new IOException(
                        "IPC read thread interrupted",
                        e
                );
            }
        }

        if(status == PipeStatus.DISCONNECTED)
        {
            throw new IOException(
                    "Disconnected!"
            );
        }

        if(status == PipeStatus.CLOSED)
        {
            return new Packet(
                    Packet.OpCode.CLOSE,
                    null
            );
        }

        Packet.OpCode op =
                Packet.OpCode.values()[
                        Integer.reverseBytes(
                                file.readInt()
                        )
                ];

        int len =
                Integer.reverseBytes(
                        file.readInt()
                );

        byte[] d = new byte[len];

        file.readFully(d);

        Packet p = new Packet(
                op,
                new JSONObject(new String(d))
        );

        LOGGER.debug(
                "Received packet: {}",
                p
        );

        if(listener != null)
        {
            listener.onPacketReceived(
                    ipcClient,
                    p
            );
        }

        return p;
    }

    @Override
    protected void closePipe() throws IOException
    {
        LOGGER.debug("Closing IPC pipe...");

        try
        {
            if(status == PipeStatus.CONNECTED)
            {
                send(
                        Packet.OpCode.CLOSE,
                        new JSONObject(),
                        null
                );
            }
        }
        catch(Exception ignored) {}

        status = PipeStatus.CLOSED;

        try
        {
            file.close();
        }
        catch(Exception ignored) {}
    }
}
