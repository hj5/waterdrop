package io.github.interestinglab.waterdrop;

import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException;
import io.github.interestinglab.waterdrop.config.CommandLineArgs;
import io.github.interestinglab.waterdrop.config.CommandLineUtils;
import io.github.interestinglab.waterdrop.common.config.Common;
import io.github.interestinglab.waterdrop.config.ConfigBuilder;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import io.github.interestinglab.waterdrop.utils.AsciiArtUtils;
import io.github.interestinglab.waterdrop.utils.Engine;
import io.github.interestinglab.waterdrop.utils.PluginType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import scala.None;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scopt.OptionParser;

import java.util.Arrays;
import java.util.List;

import static io.github.interestinglab.waterdrop.utils.Engine.SPARK;

public class Waterdrop {

    public static void main(String[] args) {
        OptionParser<CommandLineArgs> sparkParser = CommandLineUtils.sparkParser();
        run(sparkParser, SPARK, args);
    }

    /**
     * 交互态执行入口
     * @param session
     * @param config
     * @param variable
     */
    public static void exec(Object session, String config, String variable) {
        OptionParser<CommandLineArgs> sparkParser = CommandLineUtils.sparkInterpreterParser();
        String[] args = {"--config", "", "--variable", ""};
        args[1] = config;
        args[3] = variable;
        run(session, sparkParser, SPARK, args);
    }

    public static void run(OptionParser<CommandLineArgs> parser,Engine engine,String[] args){
        run(null, parser, engine, args);
    }
    public static void run(Object session, OptionParser<CommandLineArgs> parser,Engine engine,String[] args){
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(args).iterator()).asScala().toSeq();
        Option<CommandLineArgs> option = parser.parse(seq, new CommandLineArgs("client", "application.conf","" , false));
        if (option.isDefined()) {
            CommandLineArgs commandLineArgs = option.get();
            Common.setDeployMode(commandLineArgs.deployMode());
            String configFilePath = getConfigFilePath(commandLineArgs, engine);

            try {
                variable2SysProp(commandLineArgs.variable());
                boolean testConfig = commandLineArgs.testConfig();
                if (testConfig) {
                    new ConfigBuilder(configFilePath).checkConfig();
                    System.out.println("config OK !");
                } else {
                    entryPoint(session, configFilePath, engine);
                }
            } catch (ConfigRuntimeException e) {
                showConfigError(e);
                throw e;
            }catch (Exception e){
                showFatalError(e);
                throw e;
            }
        }
    }

    private static void variable2SysProp(String variable) {

        if (StringUtils.isEmpty(variable)){
            return;
        }

        for (String kv : variable.split(",")){
            String[] kvArr = kv.split("=");
            if (kvArr.length == 2){
                System.setProperty(kvArr[0], kvArr[1]);
            } else {
                throw new ConfigRuntimeException("config '--variable' param value("+kv+") format error");
            }
        }

    }

    private static String getConfigFilePath(CommandLineArgs cmdArgs, Engine engine) {
        String path = null;
        switch (engine) {
            case FLINK:
                path = cmdArgs.configFile();
                break;
            case SPARK:
                final Option<String> mode = Common.getDeployMode();
                if (mode.isDefined() && "cluster".equals(mode.get())) {
                    path = new Path(cmdArgs.configFile()).getName();
                } else {
                    path = cmdArgs.configFile();
                }
                break;
            default:
                break;
        }
        return path;
    }

    private static void entryPoint(String configFile, Engine engine) {
        entryPoint(null, configFile, engine);
    }

    private static void entryPoint(Object session, String configFile, Engine engine) {

        ConfigBuilder configBuilder = new ConfigBuilder(session, configFile, engine);
        List<BaseSource> sources = configBuilder.createPlugins(PluginType.SOURCE);
        List<BaseTransform> transforms = configBuilder.createPlugins(PluginType.TRANSFORM);
        List<BaseSink> sinks = configBuilder.createPlugins(PluginType.SINK);
        Execution execution = configBuilder.createExecution();
        baseCheckConfig(sources, transforms, sinks);
        prepare(configBuilder.getEnv(), sources, transforms, sinks);
        if (session == null) {
            showWaterdropAsciiLogo();
        }

        execution.start(sources, transforms, sinks);
    }

    private static void baseCheckConfig(List<? extends Plugin>... plugins) {
        boolean configValid = true;
        for (List<? extends Plugin> pluginList : plugins) {
            for (Plugin plugin : pluginList) {
                CheckResult checkResult = null;
                try {
                    checkResult = plugin.checkConfig();
                } catch (Exception e) {
                    checkResult = new CheckResult(false, e.getMessage());
                }
                if (!checkResult.isSuccess()) {
                    configValid = false;
                    System.out.println(String.format("Plugin[%s] contains invalid config, error: %s\n"
                            , plugin.getClass().getName(), checkResult.getMsg()));
                }
                if (!configValid) {
                    System.exit(-1); // invalid configuration
                }
            }
        }
    }

    private static void prepare(RuntimeEnv env, List<? extends Plugin>... plugins) {
        for (List<? extends Plugin> pluginList : plugins) {
            pluginList.forEach(plugin -> plugin.prepare(env));
        }

    }

    private static void showWaterdropAsciiLogo() {
        AsciiArtUtils.printAsciiArt("Waterdrop");
    }

    private static void showConfigError(Throwable throwable) {
        System.out.println(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        System.out.println("Config Error:\n");
        System.out.println("Reason: " + errorMsg + "\n");
        System.out.println(
                "\n===============================================================================\n\n\n");
    }

    private static void showFatalError(Throwable throwable) {
        System.out.println(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        System.out.println("Fatal Error, \n");
        System.out.println("Reason: " + errorMsg + "\n");
        System.out.println("Exception StackTrace: " + ExceptionUtils.getStackTrace(throwable));
        System.out.println(
                "\n===============================================================================\n\n\n");
    }
}
