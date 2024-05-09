package io.github.dtolmachev1;

import io.github.dtolmachev1.configuration.XmlConfiguration;
import io.github.dtolmachev1.data.database.Database;
import io.github.dtolmachev1.inference.manager.GenericInferenceManager;
import io.github.dtolmachev1.inference.manager.InferenceManager;
import io.github.dtolmachev1.repository.destination.DestinationRepository;
import io.github.dtolmachev1.repository.destination.DestinationRepositoryFactory;
import io.github.dtolmachev1.repository.destination.PostgresqlRepository;
import io.github.dtolmachev1.repository.source.CsvRepository;
import io.github.dtolmachev1.repository.source.SourceRepository;
import io.github.dtolmachev1.repository.source.SourceRepositoryFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class Application {
    private static final String APPLICATION_NAME = "SS2R";
    private static final int DEFAULT_CONSOLE_WIDTH = 80;
    private static final String DEFAULT_ANALYZES = "analyzes.xml";
    private static final String DEFAULT_CONFIGURATION = "config.xml";
    private final Options options;
    private final CommandLineParser parser;
    private final HelpFormatter formatter;
    private Path source;
    private Path analyzes;
    private String destination;
    private Path configuration;
    private boolean analyze;
    private boolean transform;

    public Application() {
        this.options = buildOptions();
        this.parser = new DefaultParser();
        this.formatter = new HelpFormatter();
        this.analyzes = Paths.get(DEFAULT_ANALYZES);
        this.configuration = Paths.get(DEFAULT_CONFIGURATION);
    }

    public void start(String[] args) {
        try {
            processCommands(this.parser.parse(this.options, args));
        } catch (ParseException e) {
            throw new RuntimeException("InvalidArgument");
        }
        if (Objects.nonNull(this.source) && Objects.nonNull(this.analyzes) && Objects.nonNull(this.destination) && Objects.nonNull(this.configuration)) {
            processData();
        }
    }

    private Options buildOptions() {
        Option source = Option.builder("s")
                .longOpt("source")
                .argName("sourcePath")
                .hasArg()
                .desc("Specify the source path from which the data will be read")
                .build();
        Option analyzes = Option.builder("a")
                .longOpt("analyzes")
                .argName("analyzesPath")
                .hasArg()
                .desc("Specify the analyzes file for storing and reading analysis results")
                .build();
        Option destination = Option.builder("d")
                .longOpt("destination")
                .argName("destinationDatabase")
                .hasArg()
                .desc("Specify the destination database to which the data will be written")
                .build();
        Option configuration = Option.builder("c")
                .longOpt("config")
                .argName("configPath")
                .hasArg()
                .desc("Specify the configuration file for application")
                .build();
        Option analyze = Option.builder("A")
                .longOpt("analyze")
                .desc("Perform only the analysis phase")
                .build();
        Option transform = Option.builder("T")
                .longOpt("transform")
                .desc("Perform only the transformation phase")
                .build();
        Option help = new Option("h", "help", false, "Print help information");
        Options options = new Options();
        options.addOption(source);
        options.addOption(analyzes);
        options.addOption(destination);
        options.addOption(configuration);
        options.addOption(analyze);
        options.addOption(transform);
        options.addOption(help);
        return options;
    }

    private void processCommands(CommandLine cmd) {
        if (cmd.hasOption("s") || cmd.hasOption("source")) {
            processSource(cmd);
        }
        if (cmd.hasOption("a") || cmd.hasOption("analyzes")) {
            processAnalyzes(cmd);
        }
        if (cmd.hasOption("d") || cmd.hasOption("destination")) {
            processDestination(cmd);
        }
        if (cmd.hasOption("c") || cmd.hasOption("config")) {
            processConfiguration(cmd);
        }
        if (cmd.hasOption("A") || cmd.hasOption("analyze")) {
            processAnalyze();
        }
        if (cmd.hasOption("T") || cmd.hasOption("transform")) {
            processTransform();
        }
        if (cmd.hasOption("h") || cmd.hasOption("help")) {
            processHelp();
        }
        if (!cmd.hasOption("analyze") && !cmd.hasOption("transform")) {
            this.analyze = true;
            this.transform = true;
        }
        if (cmd.getOptions().length == 0) {
            processUsage();
        }
    }

    private void processSource(CommandLine cmd) {
        this.source = Paths.get(cmd.getOptionValue("source"));
    }

    private void processAnalyzes(CommandLine cmd) {
        this.analyzes = Paths.get(cmd.getOptionValue("analyzes"));
    }

    private void processDestination(CommandLine cmd) {
        this.destination = cmd.getOptionValue("destination");
    }

    private void processConfiguration(CommandLine cmd) {
        this.configuration = Paths.get(cmd.getOptionValue("config"));
    }

    private void processAnalyze() {
        this.analyze = true;
    }

    private void processTransform() {
        this.transform = true;
    }

    private void processHelp() {
        this.formatter.printHelp(APPLICATION_NAME, this.options);
    }

    private void processUsage() {
        try (PrintWriter writer = new PrintWriter(System.out)) {
            this.formatter.printUsage(writer, DEFAULT_CONSOLE_WIDTH, APPLICATION_NAME, this.options);
        }
    }

    private void processData() {
        try (InputStream configurationInputStream = Files.newInputStream(this.configuration)) {
            XmlConfiguration.load(configurationInputStream);
            SourceRepository sourceRepository = SourceRepositoryFactory.getSourceRepository(CsvRepository.REPOSITORY_NAME);
            DestinationRepository destinationRepository = DestinationRepositoryFactory.getDestinationRepository(PostgresqlRepository.REPOSITORY_NAME);
            InferenceManager inferenceManager = new GenericInferenceManager();
            Database database = sourceRepository.load(this.source, this.destination);
            if (this.analyze) {
                try (OutputStream analyzesOutputStream = Files.newOutputStream(this.analyzes)) {
                    inferenceManager.analyzeDatabase(database);
                    inferenceManager.saveAnalyzes(analyzesOutputStream);
                } catch (IOException e) {
                    throw new RuntimeException("Unable to save analyzes");
                }
            } else {
                try (InputStream analyzesInputStream = Files.newInputStream(this.analyzes)) {
                    inferenceManager.loadAnalyzes(analyzesInputStream, database);
                } catch (IOException e) {
                    throw new RuntimeException("Unable to read analyzes");
                }
            }
            if (this.transform) {
                destinationRepository.save(database);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to read configuration");
        }
    }
}
