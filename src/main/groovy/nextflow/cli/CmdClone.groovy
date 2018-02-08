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
 * CLI sub-command clone
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
@Command (name = "clone", description = "Clone a project into a folder", abbreviateSynopsis = true)
class CmdClone extends CmdBase implements HubOptions {

    @Parameters(arity = "1..*", description = "Name of the project to clone")
    List<String> args

    @Option(names=['-r'], description = 'Revision to clone - It can be a git branch, tag or revision number')
    String revision

    @Override
    void run() {
        // the pipeline name
        String pipeline = args[0]
        final manager = new AssetManager(pipeline, this)

        // the target directory is the second parameter
        // otherwise default the current pipeline name
        def target = new File(args.size()> 1 ? args[1] : manager.getBaseName())
        if( target.exists() ) {
            if( target.isFile() )
                throw new AbortOperationException("A file with the same name already exists: $target")
            if( !target.empty() )
                throw new AbortOperationException("Clone target directory must be empty: $target")
        }
        else if( !target.mkdirs() ) {
            throw new AbortOperationException("Cannot create clone target directory: $target")
        }

        manager.checkValidRemoteRepo()
        print "Cloning ${manager.project}${revision ? ':'+revision:''} ..."
        manager.clone(target, revision)
        print "\r"
        println "${manager.project} cloned to: $target"
    }
}
