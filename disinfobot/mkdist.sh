#! /bin/sh
#
# Copyright 2004 Kate Turner
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# $Id$

ALLDIST="*.jar mkdist.sh infobot infobot.xml.example"
SRCDIST="`find org -name \*.java` `find JLinAlg -name \*.java` `find com -name \*.java`"
BINDIST="`find org -name \*.class` `find JLinAlg -name \*.class` `find com -name \*.class`"

rm -rf _dist

if [ $1 = "bin" ]; then
	DISTFILES="$ALLDIST $BINDIST"
	FILENAME=disinfobot-bin.tgz
else
	DISTFILES="$ALLDIST $SRCDIST"
	FILENAME=disinfobot.tgz
fi

for dir in `find . -type d`; do
	mkdir -p _dist/infobot/$dir
done

for file in $DISTFILES; do
	cp $file _dist/infobot/$file
done

(cd doc; tar cf - .)|(cd _dist/infobot/doc; tar xf -)

find _dist/infobot -type d -empty | xargs rmdir

(cd _dist; tar zcvf $FILENAME infobot)
mv _dist/$FILENAME ..
rm -rf _dist
ls -l ../$FILENAME
