package com.jagrosh.discordipc.entities.pipe;

import com.jagrosh.discordipc.IPCClient;
import com.jagrosh.discordipc.entities.Callback;
import com.jagrosh.discordipc.entities.Packet;

import org.json.JSONException;
import org.json.JSONObject;

import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;

import java.nio.file.Paths;

import java.util.HashMap;

public class UnixPipe extends Pipe
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UnixPipe.class);

    private final AFUNIXSocket socket;

    UnixPipe(
            IPCClient ipcClient,
            HashMap<String, Callback> callbacks,
            String location
    ) throws IOException {

        super(ipcClient, callbacks);

        socket
