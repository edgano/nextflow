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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import nextflow.scm.AssetManager
import nextflow.CommandLine.Command
import nextflow.CommandLine.Option
import nextflow.CommandLine.Parameters

/**
 * CLI sub-command PULL
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
@Command(name = "pull", description = "Download or update a project", abbreviateSynopsis = true)
class CmdPull extends CmdBase implements HubOptions {

    @Parameters(description = 'Project name or repository url to pull', arity = '0..1',paramLabel = "Name") // 0 bc we can call --all
    List<String> args

    @Option(names=['--all'], description = 'Update all downloaded projects', arity = '0')
    boolean all

    @Option(names=['-r','--revision'], description = 'Revision of the project to run (either a git branch, tag or commit SHA number)',paramLabel = "RevisionID")
    String revision

    /* only for testing purpose */
    protected File root

    @Override
    void run() {

        if( !all && !args )
            throw new AbortOperationException('Missing argument')

        def list = all ? AssetManager.list() : args.toList()
        if( !list ) {
            log.info "(nothing to do)"
            return
        }

        /* only for testing purpose */
        if( root ) {
            AssetManager.root = root
        }

        list.each {
            log.info "Checking $it ..."
            def manager = new AssetManager(it, this)

            def result = manager.download(revision)
            manager.updateModules()

            def scriptFile = manager.getScriptFile()
            String message = !result ? " done" : " $result"
            message += " - revision: ${scriptFile.revisionInfo}"
            log.info message
        }

    }

}
