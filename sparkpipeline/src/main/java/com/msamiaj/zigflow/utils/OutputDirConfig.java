package com.msamiaj.zigflow.utils;

import java.io.File;
import java.nio.file.Paths;

public final class OutputDirConfig {
	private OutputDirConfig() {
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
							.resolve(outputFile).toString()));
					break;
				}
			}
		}
	}
}

// File dir = new File(Settings.outputPath + "/" + sourceOutFolder);
// file.renameTo(new File(Settings.outputPath + "/" + outFileNewName));