package com.msamiaj.zigflow.utils;

import java.io.File;
import java.nio.file.Paths;

public final class OutputDirConfig {
	private OutputDirConfig() {
		throw new UnsupportedOperationException("OutputDirConfig class must not be instantiated!");
	}

	/**
	 * @param sourceOutFolder Name of the folder where original file resides, Only
	 *                        provide folder name not the absolute path name to
	 *                        that folder.
	 * @param outFileExt      File type or extension eg -> txt, csv
	 * @param outFileNewName  New name for the file.
	 */
	public static void renameOutputfiles(String sourceOutFolder, String outFileExt, String outFileNewName) {
		final String outputFile = outFileNewName + "." + outFileExt;
		File dir = new File(
				Paths.get(Settings.outputPath).resolve(sourceOutFolder).toString());
		if (dir.isDirectory()) {
			for (File file : dir.listFiles()) {
				if (file.getName().endsWith("." + outFileExt)) {
					file.renameTo(new File(Paths.get(Settings.outputPath)
							.resolve(sourceOutFolder).resolve(outputFile).toString()));
				} else
					file.delete();
			}
		}
	}
}
