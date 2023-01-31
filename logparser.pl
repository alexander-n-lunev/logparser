#!perl
use strict;
use DBI;

my $dbname = "logs";
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname", 'postgres', 'postgres') or die "can't connect to db $dbname";

init_db($dbh);

my $file = shift;

usage() and exit if not $file;

open (my $fh, '<', $file) or die "can't open file $file";

my $sth_message = $dbh->prepare('insert into message (created, id, int_id, str) values(?,?,?,?)');
my $sth_log = $dbh->prepare('insert into log (created, int_id, str, address) values(?,?,?,?)');

while (my $line = <$fh>) {
  my @log = split(' ', $line);
  my $string = join(' ', @log[2..(scalar @log)]);
  my $time = join(' ',@log[0..1]);
  if ($log[3] eq '<=' and $log[7] eq 'P=esmtp') {
    my $id = $1 if ($log[9] =~ /^id=(.*)$/);
    $sth_message->execute($time, $id, $log[2], $string);
  } else {
    $sth_log->execute($time, $log[2], $string, $log[4]);
  }
}

exit;

sub usage {
  print "Usage: \n";
  print "$0 <file>\n";
}

sub init_db {
  my $dbh = shift;
  my $ti_sth = $dbh->table_info(undef, undef, 'message', 'TABLE');
  if (not scalar @{ $ti_sth->fetchall_arrayref }) {
    warn "CREATING DB TABLES";
    my $create = q{
    CREATE TABLE message (
    created TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    id VARCHAR NOT NULL,
    int_id CHAR(16) NOT NULL,
    str VARCHAR NOT NULL,
    status BOOL,
    CONSTRAINT message_id_pk PRIMARY KEY(id)
    );
    CREATE INDEX message_created_idx ON message (created);
    CREATE INDEX message_int_id_idx ON message (int_id);
    CREATE TABLE log (
    created TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    int_id CHAR(16) NOT NULL,
    str VARCHAR,
    address VARCHAR
    );
    CREATE INDEX log_address_idx ON log USING hash (address);
    };
    
    $dbh->do($create);
    if ($dbh->err) {
      warn "DB CREATE ERROR $dbh->errstr";
    }
  }
}
