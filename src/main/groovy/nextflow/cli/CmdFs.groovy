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

import java.nio.charset.Charset
import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
import nextflow.file.FilePatternSplitter
import nextflow.CommandLine.Command
import nextflow.CommandLine.Option
import nextflow.CommandLine.Parameters
/**
 * Implements file system management command
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
@Command(name = "fs", abbreviateSynopsis = true, description = 'Manage common file system operations')
class CmdFs extends CmdBase {

    private List<? extends CmdBase> commands = [

            new CmdFsCopy(),
            new CmdFsMove(),
            new CmdFsList(),
            new CmdFsCat(),
            new CmdFsRemove()

    ]

    @PackageScope
    List<? extends CmdBase> getSubCommands() { Collections.unmodifiableList(commands) }

    @Override
    void run() {
        usage()
    }
}

/**
 * Copy one or more files
 */
@CompileStatic
@Command(name='cp', description = 'copy a file')
class CmdFsCopy extends CmdBase {

    @Parameters(arity = '1', index = '0', description = 'source path')
    String source

    @Parameters(arity = '1', index = '1', description = 'target path')
    String target

    @Override
    void run() {
        FilesEx.copyTo(source as Path, target as Path)
    }

}

/**
 * Move one or more directories
 */
@CompileStatic
@Command(name='mv', description = 'move a file')
class CmdFsMove extends CmdBase {

    @Parameters(arity = '1', index = '0', description = 'source path')
    String source

    @Parameters(arity = '1', index = '1', description = 'target path')
    String target

    @Override
    void run() {
        FilesEx.moveTo(source as Path, target as Path)
    }

}

/**
 * List the content of one or more directories
 */
@Slf4j
@CompileStatic
@Command(name='ls', description = 'list the content of a directory')
class CmdFsList extends CmdBase {

    static enum Type { file, dir, any }

    @Parameters(description = 'One or more paths to list')
    List<String> paths

    @Option(names=['-r','--recursive'], description = 'list directory content in a recursive manner')
    boolean recursive

    @Option(names=['--type'], description = 'show file, directories of both')
    Type type = Type.any

    private boolean followLinks

    @Option(names=['--max-depth'], description = 'max depth when traversing directories')
    private Integer maxDepth

    static final private DateTimeFormatter FMT_TIME = DateTimeFormatter.ofPattern("MMM dd HH:mm")

    static final private DateTimeFormatter FMT_YEAR = DateTimeFormatter.ofPattern("MMM dd  yyyy")

    static final private String FMT_BLANK = ''.padLeft(12)

    static final private int THIS_YEAR = Instant.now().atZone(ZoneId.systemDefault()).year

    @Override
    void run() {
        log.debug "listing paths: $paths"
        if( !paths )
            paths = ['.']

        for( String it : paths ) {
            list(it)
        }
    }

    protected void list(String it) {
        try {
            final splitter = FilePatternSplitter.glob().parse(it)
            if( splitter.isPattern() ) {
                def base = splitter.scheme ? "${splitter.scheme}://${splitter.parent}" : splitter.parent
                traverseDir(base as Path, splitter.fileName)
            }
            else {
                list0(it as Path)
            }
        }
        catch( AccessDeniedException e ) {
            throw new AbortOperationException("Cannot access path: $e.message -- check file access permissions")
        }
        catch( NoSuchFileException e ) {
            throw new AbortOperationException("File path does not exists: $e.message")
        }
    }

    protected void list0(Path path) {
        if( path.isDirectory() ) {
            final pattern = recursive ? '**' : '*'
            traverseDir(path.complete(), pattern)
        }
        else {
            listFile(path)
        }
    }

    protected void traverseDir(Path path, String pattern) {

        def opts = [:]
        opts.maxDepth = getMaxDepth(maxDepth, pattern)
        opts.relative = true
        opts.type = type.toString()
        opts.hidden = true

        FileHelper.visitFiles(opts, path, pattern) { Path it, BasicFileAttributes attr -> listFile(it,attr) }
    }

    protected void listFile(Path path, BasicFileAttributes attr=null) {
        if( !attr )
            attr = Files.readAttributes(path, BasicFileAttributes)
        def result = new StringBuilder()
        result << size0(attr) << ' '
        result << date0(attr) << ' '
        result << name0(attr, path)
        println result.toString()
    }

    protected String date0(BasicFileAttributes attr) {
        def time = attr?.lastModifiedTime()?.toInstant()?.atZone( ZoneId.systemDefault() )
        if( !time )
            return FMT_BLANK
        if( time.year == THIS_YEAR )
            return FMT_TIME.format(time)
        else
            return FMT_YEAR.format(time)
    }

    protected String size0(BasicFileAttributes attr) {
        def result = attr ? String.valueOf(attr.size()) : ''
        return result.padLeft(10)
    }

    protected String name0(BasicFileAttributes attr, Path path) {
        attr?.isDirectory() ? "${path}/" : path.toString()
    }

    private int getMaxDepth( Integer value, String filePattern ) {

        if( value != null )
            return value as int

        if( filePattern?.contains('**') )
            return Integer.MAX_VALUE

        if( filePattern?.contains('/') )
            return filePattern.split('/').findAll { it }.size()-1

        return 0
    }

}

/**
 * Print the content of one or more files
 */
@CompileStatic
@Command(name='cat', description = 'concatenate and print files')
class CmdFsCat extends CmdBase {

    @Parameters(description = 'one or more file paths to print')
    List<String> paths

    @Override
    void run() {
        for( String p : paths ) {
            cat0( p as Path )
        }
    }

    protected void cat0(Path path) {
        String line
        def reader = Files.newBufferedReader(path, Charset.defaultCharset())
        while( (line = reader.readLine()) != null )
            println line
    }

}

/**
 * Delete one or more files a directories
 */
@CompileStatic
@Command(name='rm', description = 'delete one or more files')
class CmdFsRemove extends CmdBase {

    @Parameters(description = 'one or more paths to delete')
    List<String> paths

    @Option(names=['-r','--recursive'], description = 'remove the content of a directory in a recursive manner')
    boolean recursive

    @Override
    void run() {
        for( String it : paths) {
            delete(it as Path)
        }

    }

    protected void delete(Path path) {
        if( path.isDirectory() ) {
            if( recursive )
                FilesEx.deleteDir(path)
            else
                new AbortOperationException("path ${path.toUriString()} is a directory -- use -r option to delete it along its content")
        }
        else {
            Files.deleteIfExists(path)
        }
    }

}
