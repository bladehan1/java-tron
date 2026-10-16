package org.tron.plugins;

import picocli.CommandLine;

@CommandLine.Command(name = "block",
    mixinStandardHelpOptions = true,
    description = "Export block data for offline replay.",
    subcommands = {CommandLine.HelpCommand.class, DbBlockExport.class})
public class DbBlock {
}
