 /* -------------------------------------------------------------------------------------------------------------
 A Doxygen filter which annotates comments in a C file.

 The goal is to take an existing, non-doxygen comment style and turn it into doxygen comments.
 It is packaged as a filter. The commented code never gets changed, but doxygen converts
 the comments "on the fly".

 The idea is to identify all comments in the code while keeping track of a small amount of context from
 around the comment.  The context allows us to determine:
    - Is the comment at the beginning of the file?
    - Is the comment at the global level in the file?
    - Is the comment inside a struct/union/enum?
    - Does the comment trail other statements? Or is it in front of other statements. (What about inside?)

 Basically, the filter does the following:
     - A comment at the start of a file gets the @FILE tag added.
       The tag is necessary for doxygen to recognize C code.
       By convention, this first comment describes the overall purpose of the file.
    -  Doxygen comments are passed on through.
       Doxygen comments include "/// ..." and /x*** ... /"  and style comments.
    -  Regular comments at the global level and inside structs/enums/unions are converted into doxygen comments.
       The assumption is these comments describe fields and entities like procedures, variables and macros.
    -  Trailing comments are associated with the previous item.
           "int Foo; // Comment about Foo"  becomes a doxygen comment describing Foo.

 The filter doesn't handle all existing doxygen comments. For example, it doesn't recognize the grouping tags
     "//{"  and "//}", nor does it passively echo other doxygen constructs.  Also, it has a very limited
     understanding of C syntax. For now, it is enough to get by.

 TODO? respect conditional compilation, add a File comment if none exists, be robust in not altering existing doxygen comments.
 ------------------------------------------------------------------------------------------------------------------*/
%option noyywrap nounput noinput
%{
#include <stdbool.h>
%}
    /* Variables used to track the context around a comment. */
    int lines = 0; /* How many lines of code have been processed. */
    int level = 0;  /* Level of nesting within brackets. Used to detect globals. */
    bool trailing = false;  /* Is the comment at the end of a statement? */
    bool complexData = false; // Are we inside a global complex data type (struct,union,enum)?
    char *fileName;  // The name of the file we are filtering.

    /* Forward reference. Process comments when they are discovered. */
    static void scanComment(char *comment);

 /* Regular expression patterns for the scanner. */
QuotedString                            \"([^\\\"]|\\.)*\"
CharacterLiteral                        '(\\[tvrnafb\\]|[^\\'])'
Identifier                              [a-zA-Z_$]+[a-zA-Z_$0-9]*
MultiLineComment                        [/][*][^*]*[*]+([^*/][^*]*[*]+)*[/]
SingleLineComment                       "//".*
Comment                                 {SingleLineComment}|{MultiLineComment}
Define                                  ^#define(.*\\\n)*(.*)
%%

  /* Complex data types */              /* Make note when we are inside one. Finishes with a global semicolon */
struct|union|enum                       ECHO; if (level == 0) complexData = true;
";"                                     ECHO; if (level == 0) complexData = false; trailing = true;

  /* Other keywords or identifiers */   /* Pass them through so they don't get confused with keywords */
{Identifier}                            ECHO;

  /* Quoted string literals. */         /* Pass them through. */
{QuotedString}                          ECHO;

  /* Character literals */              /* Pass them through. */
{CharacterLiteral}                      ECHO;

   /* Comments. */                      /* Scan and convert to doxygen as needed. */
{Comment}                               scanComment(yytext);

  /* Multi line #define ... */          /* Skip over #defines since they may contain unbalanced braces. */
{Define}                                ECHO;  trailing=true;

  /* New line. */                       /* Any subsequent comment is leading, and we are no longer at start of file. */
"\n"                                    ECHO; lines++; trailing = false;  //fprintf(stderr, "  lines=%d  level=%d  complex=%d   ", lines, level, complexData);

 /* Track nesting depth. */             /* Are we at the global level or not? Are we in arg list or not? */
                                        /*   We assume well formed code, so a paranthesis will never match a brace. */
"{"|"("                                 ECHO; level++;
"}"|")"                                 ECHO; level--; if (level == 0) complexData = false; trailing = true;

 /* Anything else */                    /* Pass it through */
.                                       ECHO;

%%
#include <stdio.h>
#include <string.h>
#include <errno.h>

/*
 * Custom banner character to be removed from comments.
 * We'll hardcode it to suit postgreSQL, but it should be set through a command line arg.
 */
char customBanner = '-';

/*
 * A doxygen "filter" which annotates existing C comments with doxygen directives.
 * It is flex scanner which reads a file, updates the comments, and writes to stdout.
*/
int main(int argc, char**argv) {

    /* Verify we have a single argument. */
    if (argc != 2) {
        fprintf(stderr, "Please run as DoxygenFilter <filename>\n");
        exit(1);
    }

    /* Try to open the file as stdin */
    fileName = argv[1];
    if (freopen(fileName, "r", stdin) == NULL) {
        fprintf(stderr, "Unable to open file %s: %s\n", fileName, strerror(errno));
        exit(1);
    }

    /* Now, parse the file, sending output to stdout. */
    return yylex();
}

/*
 * Output a string.
 */
static void putstr(char *str) {
    fputs(str, stdout);
}


/*
 * Remove a banner from the comment, where a banner is a sequence of two or more designated characters.
 */
static void removeBanner(char* comment, char c) {
    char *writePtr = comment + 2;
    char *readPtr = comment + 2;
    bool twoInARow = false;

    /* Scan across the comment, skipping two leading chars, moving non-banner characters forward. */
    for (; *readPtr != '\0'; readPtr++) {
        twoInARow = (readPtr[0] == c) && (readPtr[1] == c || twoInARow);
        if (!twoInARow)
            *writePtr++ = *readPtr;
    }

    /*
     * Special case for end of a C comment.
     * We may have deleted the final '*' of a C comment.
     * Ensure the last two characters are '*' and '/'.
     */
    if (comment[0] == '/' && comment[1] == '*' && writePtr[-2] != '*')
    {
        writePtr--;
        *writePtr++ = '*';
        *writePtr++ = '/';
    }

    *writePtr = '\0';
}

/*
 * Process comments as they are encountered in the text.
 *
 * Uses context information gathered during scanning to decide if the comment should be
 * converted to a doxygen comment.
 */
static void scanComment(char *comment) {

    /* Echo the starting part of the comment, either // or / *.  */
    putchar(comment[0]); putchar(comment[1]);

    /* If at global level or inside a complex data declarations. TODO: but not inside a statement.*/
    if (level == 0 || complexData) {

        /* Ensure this is a doxygen comment by repeating the 2nd character. */
        putchar(comment[1]);  // Note: adds harmless extra char to existing doxygen comment.

        /* If there is a preceding stmt on the line, then associate the doxygen comment with that stmt. */
        if (trailing)
            putstr("< ");

        /* If at the start of the file, add the @FILE tag. TODO: should it be at end of comment? */
        if (lines == 0)
            printf(" @file %s ", fileName);
    }

    /*
     * Remove banners from the comment. A banner is a special character, repeated at least twice.
     */
    removeBanner(comment, '/');
    removeBanner(comment, '*');
    removeBanner(comment, customBanner);

    /* finish outputting the comment. The first two chars were already output. */
    putstr(comment+2);
}
