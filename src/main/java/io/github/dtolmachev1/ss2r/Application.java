package io.github.dtolmachev1.ss2r;

import io.github.dtolmachev1.ss2r.configuration.XmlConfiguration;
import io.github.dtolmachev1.ss2r.data.database.Database;
import io.github.dtolmachev1.ss2r.repository.destination.DestinationRepository;
import io.github.dtolmachev1.ss2r.repository.destination.SqlRepository;
import io.github.dtolmachev1.ss2r.repository.source.CsvRepository;
import io.github.dtolmachev1.ss2r.repository.source.SourceRepository;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class Application {
    private final Options options;
    private final CommandLineParser parser;
    private final HelpFormatter formatter;
    private Path source;
    private String destination;
    private Path config;

    public Application() {
        this.options = buildOptions();
        this.parser = new DefaultParser();
        this.formatter = new HelpFormatter();
        this.config = Paths.get("config.xml");
    }

    public int start(String[] args) {
        try {
            processCommands(this.parser.parse(this.options, args));
        } catch (ParseException e) {
            throw new RuntimeException("Invalid argument");
        }
        if (Objects.nonNull(this.source) && Objects.nonNull(this.destination) && Objects.nonNull(this.config)) {
            processData();
        }
        return 0;
    }

    private Options buildOptions() {
        Option source = Option.builder("s")
                .longOpt("source")
                .argName("sourcePath")
                .hasArg()
                .desc("Specify the source path from which the data will be read")
                .build();
        Option destination = Option.builder("d")
                .longOpt("destination")
                .argName("destinationDatabase")
                .hasArg()
                .desc("Specify the destination database to which the data will be written")
                .build();
        Option config = Option.builder("c")
                .longOpt("config")
                .argName("configPath")
                .hasArg()
                .optionalArg(true)
                .desc("Specify the configuration file for application")
                .build();
        Option help = new Option("h", "help", false, "Print help information");
        Options options = new Options();
        options.addOption(source);
        options.addOption(destination);
        options.addOption(config);
        options.addOption(help);
        return options;
    }

    private void processCommands(CommandLine cmd) {
        if (cmd.hasOption("s") || cmd.hasOption("source")) {
            processSource(cmd);
        }
        if (cmd.hasOption("d") || cmd.hasOption("destination")) {
            processDestination(cmd);
        }
        if (cmd.hasOption("c") || cmd.hasOption("config")) {
            processConfig(cmd);
        }
        if (cmd.hasOption("h") || cmd.hasOption("help")) {
            processHelp();
        }
        if (cmd.getOptions().length == 0) {
            processUsage();
        }
    }

    private void processSource(CommandLine cmd) {
        this.source = Paths.get(cmd.getOptionValue("source"));
    }

    private void processDestination(CommandLine cmd) {
        this.destination = cmd.getOptionValue("destination");
    }

    private void processConfig(CommandLine cmd) {
        this.config = Paths.get(cmd.getOptionValue("config"));
    }

    private void processHelp() {
        this.formatter.printHelp("SS2R", this.options);
    }

    private void processUsage() {
        try (PrintWriter writer = new PrintWriter(System.out)) {
            this.formatter.printUsage(writer, 80, "SS2R", this.options);
        }
    }

    private void processData() {
        try (InputStream inputStream = Files.newInputStream(this.config)) {
            XmlConfiguration.newInstance(inputStream);
            SourceRepository sourceRepository = CsvRepository.newInstance(this.source);
            DestinationRepository destinationRepository = SqlRepository.newInstance(this.destination);
            Database database = sourceRepository.load();
            database.normalize();
            destinationRepository.save(database);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read configuration");
        }
    }
}
