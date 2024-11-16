package com.msamiaj.zigflow.utils;

import java.nio.file.Paths;

public final class Settings {
	public static final String currentWorkingDir = System.getProperty("user.dir");
	public static final String projectRoot = Paths.get(currentWorkingDir).getParent().toAbsolutePath().toString();
	public static final String checkpointPath = Paths.get(projectRoot + "/checkpoint").toAbsolutePath().toString();
	public static final String datasetsPath = Paths.get(projectRoot + "/datasets").toAbsolutePath().toString();
}
