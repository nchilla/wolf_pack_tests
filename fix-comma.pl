# stream.js accidentally leaves a comma at the end of the last object in the array for each json.
# this perl script removes it
my $filename = "/Volumes/chilla/nico/cleaned/chicago-defender.json";
my $fsize = -s $filename;  
print $fsize."\n";
open($FILE, "+<", $filename) or die $!; 
seek $FILE, $fsize-2, SEEK_SET; # or 0 (numeric) instead of SEEK_SET
print $FILE " ";
close $FILE;