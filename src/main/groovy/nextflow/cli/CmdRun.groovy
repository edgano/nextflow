/*
 * Copyright (c) 2013-2018, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2018, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.cli

import java.nio.file.Files
import java.nio.file.Path

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.GParsConfig
import nextflow.Const
import nextflow.config.ConfigBuilder
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import nextflow.k8s.K8sDriverLauncher
import nextflow.scm.AssetManager
import nextflow.script.ScriptFile
import nextflow.script.ScriptRunner
import nextflow.util.CustomPoolFactory
import nextflow.util.Duration
import nextflow.util.HistoryFile
import org.yaml.snakeyaml.Yaml

import nextflow.CommandLine
import nextflow.CommandLine.Command
import nextflow.CommandLine.ITypeConverter
import nextflow.CommandLine.Option
import nextflow.CommandLine.Parameters
/**
 * CLI sub-command RUN
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
@Command(name = "run", description = "Execute a pipeline project", abbreviateSynopsis = true)
class CmdRun extends CmdBase implements HubOptions {

    static public String NAME = 'run'

    static List<String> VALID_PARAMS_FILE = ['json', 'yml', 'yaml']

    static {
        // install the custom pool factory for GPars threads
        GParsConfig.poolFactory = new CustomPoolFactory()
    }

    static class DurationConverter implements ITypeConverter<Long> {
        @Override
        Long convert(String value) {
            if( !value ) throw new IllegalArgumentException()
            if( value.isLong() ) {  return value.toLong() }
            return Duration.of(value).toMillis()
        }
    }

    @Option(names=['--name'], description = 'assign a mnemonic name to the a pipeline run', paramLabel = "<name>")
    String runName

    @Option(names=['--lib'], description = 'library extension path', paramLabel = "<lib-path>")
    String libPath

    @Option(names=['--cache'], description = 'enable/disable processes caching', arity = '0..1')
    boolean cacheable = true

    @Option(names=['--resume'], description = 'execute the script using the cached results, useful to continue executions that was stopped by an error', arity = "0..1", paramLabel = "<session-id>")
    String resume

    @Option(names=['--ps','--pool-size'], description = 'number of threads in the execution pool', paramLabel = "<n>")
    Integer poolSize

    @Option(names=['--pi','--poll-interval'], description = 'executor poll interval (duration string ending with ms|s|m)', converter = [DurationConverter], paramLabel = "<duration>")
    long pollInterval

    @Option(names=['--qs','--queue-size'], description = 'max number of processes that can be executed in parallel by each executor', paramLabel = "<n>")
    Integer queueSize

    @Option(names=['--test'], description = 'test a script function with the name specified', paramLabel = "<function-name>")
    String test

    @Option(names=['-w', '--work-dir'], description = 'directory where intermediate result files are stored',arity = '1', paramLabel = "<work-dir>")
    String workDir


    @Option(names=['--params-file'], description = 'load script parameters from a JSON/YAML file',paramLabel = "<file-name>")
    String paramsFile

    @Option(names = ['--process'], description = 'set process options')
    Map<String,String> process = [:]

    @Option(names = ['--e'], description = 'add the specified variable to execution environment')
    Map<String,String> env = [:]

    @Option(names = ['-E'], description = 'exports all current system environment')
    boolean exportSysEnv

    @Option(names = ['--executor'], description = 'det executor options', hidden = true)
    Map<String,String> executorOptions = [:]

    @Option(names=['-r','--revision'], description = 'revision of the project to run (either a git branch, tag or commit SHA number)' )
    String revision

    @Option(names=['--latest'], description = 'pull latest changes before run')
    boolean latest

    @Option(names=['--stdin'], hidden = true)
    boolean stdin

    @Option(names = ['--with-drmaa'], description = 'enable DRMAA binding')
    String withDrmaa

    @Option(names = ['--with-trace'], description = 'create processes execution tracing file')
    String withTrace

    @Option(names = ['--with-report'], description = 'create processes execution html report')
    String withReport

    @Option(names = ['--with-timeline'], description = 'create processes execution timeline file')
    String withTimeline

    @Option(names = ['--with-singularity'], description = 'enable process execution in a Singularity container')
    def withSingularity

    @Option(names = ['--with-docker'], description = 'enable process execution in a Docker container')
    def withDocker

    @Option(names = ['--without-docker'], description = 'disable process execution with Docker', arity = '0')
    boolean withoutDocker

    @Option(names = ['--with-k8s', '-K'], description = 'enable execution in a Kubernetes cluster')
    def withKubernetes

    @Option(names = ['--with-mpi'], hidden = true)
    boolean withMpi

    @Option(names = ['--with-dag'], description = 'create pipeline DAG file')
    String withDag

    @Option(names = ['--bg'], arity = '0', description = 'launch the execution in background')
    boolean backgroundFlag

    @Option(names=['-c','--config'], hidden = true)
    List<String> runConfig

    @Option(names = ['--cluster'], description = 'set cluster options', hidden = true )
    Map<String,String> clusterOptions = [:]

    @Option(names=['--profile'], description = 'choose a configuration profile', paramLabel = "<profile-name>")
    String profile

    @Option(names=['--dump-hashes'], description = 'dump task hash keys for debugging purpose')
    boolean dumpHashes

    @Option(names=['--dump-channels'], description = 'dump channels for debugging purpose', paramLabel = "<channel-names>")
    String dumpChannels

    @Option(names=['-N','--with-notification'], description = 'send a notification email on workflow completion to the specified recipients', paramLabel = "<email-address>")
    String withNotification

    @Parameters(index = '0', arity = '1', description = 'workflow to execute, either a script file or a project repository')
    String workflow

    /**
     * Defines the parameters to be passed to the pipeline script
     */
    Map<String,String> params

    private CommandLine cli

    @Override
    protected CommandLine register(CommandLine parent) {
        this.cli = super.register(parent)
        cli.setUnmatchedArgumentsAllowed(true)
        return cli
    }

    @Override
    void run() {
        if( !test )
            params = makeParams( cli.getUnmatchedArguments() )

        if( withDocker && withoutDocker )
            throw new AbortOperationException("Command line options `--with-docker` and `--without-docker` cannot be specified at the same time")

        checkRunName()

        if( withKubernetes ) {
            // that's another story
            new K8sDriverLauncher(cmd: this, runName: runName).run(workflow)
            return
        }

        log.info "N E X T F L O W  ~  version ${Const.APP_VER}"

        // -- specify the arguments
        final scriptFile = getScriptFile(workflow)

        // create the config object
        final config = new ConfigBuilder()
                        .setOptions(launcher.options)
                        .setCmdRun(this)
                        .setBaseDir(scriptFile.parent)

        // -- create a new runner instance
        final runner = new ScriptRunner(config)
        runner.script = scriptFile
        runner.profile = profile

        if( this.test ) {
            runner.test(this.test, cli.getUnmatchedArguments() )
            return
        }

        def info = CmdInfo.status( log.isTraceEnabled() )
        log.debug( '\n'+info )

        // -- add this run to the local history
        runner.verifyAndTrackHistory(launcher.cliString, runName)

        // -- run it!
        runner.execute()
    }

    private void checkRunName() {
        if( runName == 'last' )
            throw new AbortOperationException("Not a valid run name: `last`")

        if( !runName ) {
            // -- make sure the generated name does not exist already
            runName = HistoryFile.DEFAULT.generateNextName()
        }

        else if( HistoryFile.DEFAULT.checkExistsByName(runName) )
            throw new AbortOperationException("Run name `$runName` has been already used -- Specify a different one")
    }

    protected ScriptFile getScriptFile(String pipelineName) {
        assert pipelineName

        /*
         * read from the stdin
         */
        if( pipelineName == '-' ) {
            def file = tryReadFromStdin()
            if( !file )
                throw new AbortOperationException("Cannot access `stdin` stream")

            if( revision )
                throw new AbortOperationException("Revision option cannot be used running a local script")

            return new ScriptFile(file)
        }

        /*
         * look for a file with the specified pipeline name
         */
        def script = new File(pipelineName)
        if( script.isDirectory()  ) {
            script = new AssetManager().setLocalPath(script).getMainScriptFile()
        }

        if( script.exists() ) {
            if( revision )
                throw new AbortOperationException("Revision option cannot be used running a script")
            def result = new ScriptFile(script)
            log.info "Launching `$script` [$runName] - revision: ${result.getScriptId()?.substring(0,10)}"
            return result
        }

        /*
         * try to look for a pipeline in the repnormalizeArgsository
         */
        def manager = new AssetManager(pipelineName, this)
        def repo = manager.getProject()

        boolean checkForUpdate = true
        if( !manager.isRunnable() || latest ) {
            log.info "Pulling $repo ..."
            def result = manager.download()
            if( result )
                log.info " $result"
            checkForUpdate = false
        }
        // checkout requested revision
        try {
            manager.checkout(revision)
            manager.updateModules()
            def scriptFile = manager.getScriptFile()
            log.info "Launching `$repo` [$runName] - revision: ${scriptFile.revisionInfo}"
            if( checkForUpdate )
                manager.checkRemoteStatus(scriptFile.revisionInfo)
            // return the script file
            return scriptFile
        }
        catch( AbortOperationException e ) {
            throw e
        }
        catch( Exception e ) {
            throw new AbortOperationException("Unknown error accessing project `$repo` -- Repository may be corrupted: ${manager.localPath}", e)
        }

    }

    static protected File tryReadFromStdin() {
        if( !System.in.available() )
            return null

        getScriptFromStream(System.in)
    }

    static protected File getScriptFromStream( InputStream input, String name = 'nextflow' ) {
        input != null
        File result = File.createTempFile(name, null)
        result.deleteOnExit()
        input.withReader { Reader reader -> result << reader }
        return result
    }

    Map getParsedParams() {

        def result = [:]

        if( paramsFile ) {
            def path = validateParamsFile(paramsFile)
            def ext = path.extension.toLowerCase() ?: null
            if( ext == 'json' )
                readJsonFile(path, result)
            else if( ext == 'yml' || ext == 'yaml' )
                readYamlFile(path, result)
        }

        // read the params file if any

        // set the CLI params
        params?.each { key, value ->
            result.put( key, parseParam(value) )
        }
        return result
    }

    static private parseParam( String str ) {

        if ( str == null ) return null

        if ( str.toLowerCase() == 'true') return Boolean.TRUE
        if ( str.toLowerCase() == 'false' ) return Boolean.FALSE

        if ( str.isInteger() ) return str.toInteger()
        if ( str.isLong() ) return str.toLong()
        if ( str.isDouble() ) return str.toDouble()

        return str
    }

    private Path validateParamsFile(String file) {

        def result = FileHelper.asPath(file)
        if( !result.exists() )
            throw new AbortOperationException("Specified params file does not exists: $file")

        def ext = result.getExtension()
        if( !VALID_PARAMS_FILE.contains(ext) )
            throw new AbortOperationException("Not a valid params file extension: $file -- It must be one of the following: ${VALID_PARAMS_FILE.join(',')}")

        return result
    }


    private void readJsonFile(Path file, Map result) {
        try {
            def json = (Map)new JsonSlurper().parse(Files.newInputStream(file))
            result.putAll(json)
        }
        catch( Exception e ) {
            throw new AbortOperationException("Cannot parse params file: $file", e)
        }
    }

    private void readYamlFile(Path file, Map result) {
        try {
            def yaml = (Map)new Yaml().load(Files.newInputStream(file))
            result.putAll(yaml)
        }
        catch( Exception e ) {
            throw new AbortOperationException("Cannot parse params file: $file", e)
        }
    }

    protected Map<String,String> makeParams(List<String> args) {
        if( args == null )
            return Collections.emptyMap()
        
        List<String> normalise = new ArrayList<>(args.size()*2)
        Map<String,String> result = [:]

        for( String item : args ) {
            int p = item.indexOf('=')
            if( p!=-1 && item.startsWith('--') ) {
                normalise << item.substring(0,p)
                normalise << item.substring(p+1)
            }
            else {
                normalise << item
            }
        }

        String current=null
        for( String item : normalise ) {
            if( item =~ /^--\w/ ) {
                current = item.substring(2)
            }
            else if( current ) {
                result.put(current, item)
//                def value = result.get(current)
//                if( value==null ) {
//                    result.put(current, item)
//                }
//                else if( value instanceof List ) {
//                    value.add(item)
//                }
//                else {
//                    result.put( current, [value, item] )
//                }
            }
            else {
                throw new IllegalArgumentException("Not a valid workflow parameter: $item")
            }
        }

        return result
    }

}
