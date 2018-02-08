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

import java.nio.file.Path
import java.nio.file.Paths

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.config.ConfigBuilder
import nextflow.exception.AbortOperationException
import nextflow.scm.AssetManager
import nextflow.util.ConfigHelper
import nextflow.CommandLine.Command
import nextflow.CommandLine.Option
import nextflow.CommandLine.Parameters

/**
 *  Prints the pipeline configuration
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
@Command(name = "config", description = "Print a project configuration", abbreviateSynopsis = true)
class CmdConfig extends CmdBase {

    @Parameters(description = "Project to configure",paramLabel = "Project Name")    //TODO is it mandatory?
    List<String> args = []

    @Option(names=['-a','--show-profiles'], description = 'Show all configuration profiles')
    boolean showAllProfiles

    @Option(names=['--profile'], description = 'Choose a configuration profile',paramLabel = "Profile")
    String profile

    @Option(names =['--properties'], description = 'Prints config using Java properties notation')
    boolean printProperties

    @Option(names =['--flat', '--flatten'], description = 'Print config using flat notation')
    boolean printFlatten

    private OutputStream stdout = System.out

    @Override
    void run() {
        Path base = null
        if( args ) base = getBaseDir(args[0])
        if( !base ) base = Paths.get('.')

        if( profile && showAllProfiles ) {
            throw new AbortOperationException("Option `--profile` conflicts with option `--show-profiles`")
        }

        if( printProperties && printFlatten )
            throw new AbortOperationException("Option `--flat` and `--properties` conflicts")

        def config = new ConfigBuilder()
                .setOptions(launcher.options)
                .setBaseDir(base.complete())
                .setCmdConfig(this)
                .configObject()

        if( printProperties ) {
            printProperties(config, stdout)
        }
        else if( printFlatten ) {
            printFlatten(config, stdout)
        }
        else {
            printCanonical(config, stdout)
        }
    }

    /**
     * Prints a {@link ConfigObject} using Java {@link Properties} in canonical format
     * ie. any nested config object is printed withing curly brackets
     *
     * @param config The {@link ConfigObject} representing the parsed workflow configuration
     * @param output The stream where output the formatted configuration notation
     */
    protected void printCanonical(ConfigObject config, OutputStream output) {
        output << ConfigHelper.toCanonicalString(config)
    }

    /**
     * Prints a {@link ConfigObject} using Java {@link Properties} format
     *
     * @param config The {@link ConfigObject} representing the parsed workflow configuration
     * @param output The stream where output the formatted configuration notation
     */
    protected void printProperties(ConfigObject config, OutputStream output) {
        output << ConfigHelper.toPropertiesString(config)
    }

    /**
     * Prints a {@link ConfigObject} using properties dot notation.
     * String values are enclosed in single quote characters.
     *
     * @param config The {@link ConfigObject} representing the parsed workflow configuration
     * @param output The stream where output the formatted configuration notation
    */
    protected void printFlatten(ConfigObject config, OutputStream output) {
        output << ConfigHelper.toFlattenString(config)
    }

    /**
     * Prints the {@link ConfigObject} configuration object using the default notation
     *
     * @param config The {@link ConfigObject} representing the parsed workflow configuration
     * @param output The stream where output the formatted configuration notation
     */
    protected void printDefault(ConfigObject config, OutputStream output) {
        def writer = new PrintWriter(output,true)
        config.writeTo( writer )
    }


    Path getBaseDir(String path) {

        def file = Paths.get(path)
        if( file.isDirectory() )
            return file

        if( file.exists() ) {
            return file.parent ?: Paths.get('/')
        }

        def manager = new AssetManager(path)
        manager.isLocal() ? manager.localPath.toPath() : null

    }



}
