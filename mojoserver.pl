#!perl
use strict;
use Mojolicious::Lite;
use DBI;

my $dbname = "logs";

my $dbh = DBI->connect("dbi:Pg:dbname=$dbname", 'postgres', 'postgres') or die "can't connect to db $dbname";

get '/' => sub {
  my $c = shift;
  $c->render;
} => 'index';

post '/' => sub {
  my $c = shift;
  my $search = $c->param('search');
  my $search_follow = $c->param('search_follow');
  my $rows = [];
  my $counter = 100;
  my $pusher = sub {
    my $row = shift;
    push @{ $rows }, $row;
    $counter--;
    return 1 if $counter > 0;
  };
  if ($search_follow) {
    my $sth = $dbh->prepare(qq{select created, int_id, str from log where str ~ '$search'});
    my $query_all = qq{
      (select created, int_id, str from log where int_id = ?)
      UNION
      (select created, int_id, str from message where int_id = ?)
      ORDER BY int_id, created
    };
    my $sth_inner = $dbh->prepare($query_all);
    $sth->execute();
    while (my $row = $sth->fetchrow_hashref) {
      $sth_inner->execute($row->{int_id}, $row->{int_id});
      while (my $row_inner = $sth_inner->fetchrow_hashref)  { 
        last if not $pusher->($row_inner);
      }
    }
  } else {
    my $query = qq{
      (select created, int_id, str from log where str ~ '$search')
      UNION
      (select created, int_id, str from message where str ~ '$search')
      ORDER BY int_id, created
    };
    my $sth = $dbh->prepare($query);
    $sth->execute();
    while (my $row = $sth->fetchrow_hashref) {
      last if not $pusher->($row);
    }
  }
  $c->render(rows => $rows, counter => $counter);
} => 'index';

app->start;

__DATA__
@@ index.html.ep
%= form_for '/' => (method => 'POST') => begin
  <div>Поиск адреса в БД:</div>
  %= text_field 'search'
  <div>Показать протокол</div>
  %= check_box 'search_follow'
  %= submit_button
% end
% if (stash('counter') <= 0) {
<div style="color: red">
  Показаны первые 100 записей.
</div>
% }
% if (stash('rows')) {
<div class="data">
% foreach my $row (@{ stash('rows')}) {
  <div class="line">
    <%= $row->{created} %>
    <%= $row->{str} %>
  </div>
% }
%}
</div>
