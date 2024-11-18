package com.msamiaj.zigflow.utils;

import java.nio.file.Paths;

public final class Settings {
	private static final String currentWorkingDir = System.getProperty("user.dir");
	public static final String projectRoot = Paths.get(currentWorkingDir).getParent().toAbsolutePath().toString();

	public static final String checkpointPath = Paths.get(projectRoot).resolve("checkpoint")
			.toAbsolutePath().toString();

	public static final String datasetsPath = Paths.get(projectRoot).resolve("datasets")
			.toAbsolutePath().toString();

	public static final String outputPath = Paths.get(projectRoot).resolve("output")
			.toAbsolutePath().toString();

	private Settings() {
		throw new UnsupportedOperationException("Setting class must not be instantiated!");
	}
}
