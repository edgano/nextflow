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
import nextflow.CommandLine.Command
import nextflow.CommandLine.Option

/**
 * Main application command line options
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Command (name = "nextflow", description ="Print the options", abbreviateSynopsis = true)
class CliOptions {

    /**
     * The packages to debug
     */
    @Option(names=['--debug'],hidden = true)
    List<String> debug

    @Option(names=['--log'], description = 'Set nextflow log file path', paramLabel ="<File>")
    String logFile

    @Option(names=['-c','--config'], description = 'Add the specified file to configuration set', paramLabel ="<File>")
    List<String> userConfig

    @Option(names=['-C'], description = 'Use the specified configuration file(s) overriding any defaults', paramLabel ="<File>")
    List<String> config

    /**
     * the packages to trace
     */
    @Option(names=['--trace'], hidden = true)
    List<String> trace

    /**
     * Enable syslog appender
     */
    @Option(names=['--syslog'], description = 'Send logs to syslog server (eg. localhost:514)' , paramLabel ="Destination")
    String syslog

    /**
     * Print out the version number and exit
     */
    @Option(names=['-v','--version'], description = 'Print the program version', versionHelp = true)
    boolean version //TODO we can use the 'versionHelp' @link http://picocli.info/#_help_options

    /**
     * Print out the 'help' and exit
     */
    @Option(names=['-h','--help'], description = 'Print this help', usageHelp = true)
    boolean help

    @Option(names=['-q','--quite'], description = 'Do not print information messages')
    boolean quiet

    @Option(names=['--bg'], description = 'Execute nextflow in background', arity='0')
    boolean background

    @Option(names=['-D'], description = 'Set JVM properties' , paramLabel ="<Key:Value>")
    Map<String,String> jvmOpts = [:]

    @Option(names=['-u','--self-update'], description = 'Update nextflow to the latest version', arity = '0', hidden = true)
    boolean selfUpdate

}
