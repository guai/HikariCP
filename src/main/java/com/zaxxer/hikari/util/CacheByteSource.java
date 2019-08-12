package com.zaxxer.hikari.util;

import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.InputStream;

public class CacheByteSource extends ByteSource {
	private ByteSource byteSource;

	@SneakyThrows
	public CacheByteSource(final InputStream inputStream, long size) {
		byteSource = ByteSource.wrap(ByteStreams.toByteArray(ByteStreams.limit(inputStream, size)));
	}

	@SneakyThrows
	public CacheByteSource(final InputStream inputStream) {
		byteSource = ByteSource.wrap(ByteStreams.toByteArray(inputStream));
	}

	@Override
	public InputStream openStream() throws IOException {
		return byteSource.openStream();
	}
}
