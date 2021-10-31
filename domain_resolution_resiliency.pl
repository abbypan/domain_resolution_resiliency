#!/usr/bin/perl
use strict;
use warnings;
use SimpleR::Reshape qw/write_table/;
use SimpleR::Stat qw/sum_arrayref/;
use Data::Dump qw/dump/;
use File::Slurp qw/slurp read_file write_file/;
use List::Util qw( min max );
use JSON;
use Tie::IxHash;

our $DATA_DIR             = 'data';
our $RES_DIR              = 'res';
our @DOMAIN_STATUS_METRIC = qw/clientUpdate serverUpdate clientDelete serverDelete clientTransfer serverTransfer/;
our @NS_DIVERSITY_METRIC  = qw/ns_cnt ns_ip_cnt ns_ip_as_cnt ns_ip_country_cnt ns_ip_anycast_cnt/;
our @TTL_METRIC           = qw/ns_ttl ns_ip_ttl mx_ttl mx_ip_ttl cname_ttl ip_ttl/;

my ( $qsld ) = @ARGV;
$qsld .= "." unless ( $qsld =~ /\.$/ );

get_sld( $qsld );

calc_qsld_metric( $qsld );

sub calc_qsld_metric {

  my @metrics;

  my $sr = calc_domain_status_metric( $qsld );
  dump( "domain_status", $sr );
  while ( my ( $dom, $sr_r ) = each %$sr ) {
    my $mc = 'domain_status';
    for my $m ( @DOMAIN_STATUS_METRIC ) {
      push @metrics, [ $dom, $mc, $m, $sr_r->{$mc}{$m} ];
    }
  }

  my $nr = calc_ns_diversity_metric( $qsld );
  dump( "ns", $nr );
  for my $nr_r ( @$nr ) {
    my $dom = $nr_r->{dom};
    my $mc  = 'ns_diversity';
    for my $m ( @NS_DIVERSITY_METRIC ) {
      push @metrics, [ $dom, $mc, $m, $nr_r->{ns_diversity_metric}{$m} ];
    }

    $mc = 'ttl_metric';
    for my $m ( @TTL_METRIC ) {
      push @metrics, [ $dom, $mc, $m, $nr_r->{ttl_metric}{$m} ] if ( exists $nr_r->{ttl_metric}{$m} );
    }
  }

  my $tr = calc_ttl_diversity_metric( $qsld );
  dump( "ttl", $tr );
  while ( my ( $dom, $tr_r ) = each %$tr ) {
    my $mc = 'ttl_metric';
    for my $m ( @TTL_METRIC ) {
      push @metrics, [ $dom, $mc, $m, $tr_r->{ttl_metric}{$m} ] if ( exists $tr_r->{ttl_metric}{$m} );
    }
  }

  my $head_r   = [ 'dom', 'metric_class', 'metric', 'value' ];
  my $metric_f = "$RES_DIR/metric-final-$qsld.csv";
  write_table( \@metrics, file => $metric_f, sep => '|', head => $head_r );
  return $metric_f;
} ## end sub calc_qsld_metric

sub calc_ttl_diversity_metric {
  my ( $qsld ) = @_;

  my @stat;
  my %see;
  my %mm;

  my @d = (

    #[ $qsld, 'NS' ],
    [ $qsld,       'MX' ],
    [ "www.$qsld", 'A' ],
    [ "www.$qsld", 'AAAA' ],
  );

  for my $qr ( @d ) {
    my ( $qd, $qt ) = @$qr;
    my $f           = "$DATA_DIR/query-$qd-$qt.log";
    my @record_list = map { [ split /\s+/, $_ ] } read_file( $f );

    for my $record_r ( @record_list ) {
      my ( $dom, $ttl, $class, $type, @rr_s ) = @$record_r;
      my $rr = $rr_s[-1];

      #next unless($dom eq $qd);
      next unless ( $type =~ /^A|AAAA|CNAME|MX$/ );
      if ( $type eq 'CNAME' ) {

        #push  @{$mm{$qd}{$qt}{$dom}{$rr}{cname_ttl}}, $ttl;
        my $prim = join( "|", $qd, $qt, $dom, $type, $rr );
        next if ( exists $see{$prim} );
        push @stat, [ $qd, $qt, $dom, $type, $rr, $ttl, $rr, $ttl, '', '', '', '' ];
        $see{$prim} = 1;
        next;
      } elsif ( $type eq 'MX' ) {
        push @{ $mm{$qd}{$qt}{$dom}{mx_ttl} }, $ttl;
        push @{ $mm{$qd}{mx_ttl} }, $ttl;
        for my $at ( qw/A AAAA/ ) {
          my $a_f        = "$DATA_DIR/query-$rr-$at.log";
          my $x_ip_ttl_r = parse_a_info( $rr, $a_f );
          while ( my ( $x_ip, $x_ip_ttl ) = each %$x_ip_ttl_r ) {
            my $ip_info_f = "$DATA_DIR/ipinfo-$x_ip.json";
            my $r         = parse_ipinfo( $ip_info_f );

            my $prim = join( "|", $dom, $rr, $x_ip );
            next if ( exists $see{$prim} );
            push @stat, [ $qd, $qt, $dom, $type, $rr, $ttl, $x_ip, $x_ip_ttl, $r->{org}, $r->{anycast} ? 1 : 0, $r->{country}, $r->{region} ];
            $see{$prim} = 1;
          }
        }
      } elsif ( $type eq 'A' or $type eq 'AAAA' ) {
        my $ip_info_f = "$DATA_DIR/ipinfo-$rr.json";
        my $r         = parse_ipinfo( $ip_info_f );

        my $prim = join( "|", $dom, $rr, $rr );
        next if ( exists $see{$prim} );
        push @stat, [ $qd, $qt, $dom, $type, $rr, $ttl, $rr, $ttl, $r->{org}, $r->{anycast} ? 1 : 0, $r->{country}, $r->{region} ];
        $see{$prim} = 1;
      }

    } ## end for my $record_r ( @record_list)
  } ## end for my $qr ( @d )

  #dump(\%m);
  my $head_r = [qw/qd qt dom type rr rr_ttl rr_ip rr_ip_ttl rr_ip_as rr_ip_anycast rr_ip_country rr_ip_region/];
  my $ttl_f  = "$DATA_DIR/metric-ttl-$qsld.csv";
  write_table( \@stat, file => $ttl_f, sep => '|', head => $head_r );

  my %ttl_d;
  tie %ttl_d, 'Tie::IxHash';
  push @{ $ttl_d{ $_->[0] } }, $_ for @stat;

  #dump (%ttl_d);

  while ( my ( $dom, $ttl_r ) = each %ttl_d ) {

    #print "dom : $dom\n";
    #my %m;
    #$m{dom} = $dom;

    my @mx_ttl_r = grep { $_->[3] eq 'MX' } @$ttl_r;
    push @{ $mm{ $_->[0] }{ $_->[1] }{ $_->[2] }{mx_ip_ttl} }, $_->[7] for @mx_ttl_r;
    push @{ $mm{ $_->[0] }{mx_ip_ttl} }, $_->[7] for @mx_ttl_r;

    my @cname_ttl_r = grep { $_->[3] eq 'CNAME' } @$ttl_r;
    push @{ $mm{ $_->[0] }{ $_->[2] }{ $_->[4] }{cname_ttl} }, $_->[5] for @cname_ttl_r;
    push @{ $mm{ $_->[0] }{cname_ttl} }, $_->[5] for @cname_ttl_r;

    my @ip_ttl_r = grep { $_->[3] eq 'A' or $_->[3] eq 'AAAA' } @$ttl_r;
    push @{ $mm{ $_->[0] }{ $_->[2] }{ $_->[4] }{ip_ttl} }, $_->[5] for @ip_ttl_r;
    push @{ $mm{ $_->[0] }{ip_ttl} }, $_->[5] for @ip_ttl_r;
  } ## end while ( my ( $dom, $ttl_r...))

  while ( my ( $dom, $mm_r ) = each %mm ) {
    $mm_r->{ttl_data} = $ttl_d{$dom};

    if ( exists $mm_r->{cname_ttl} ) {
      $mm_r->{min_cname_ttl} = min( @{ $mm_r->{cname_ttl} } );
      $mm_r->{ttl_metric}{cname_ttl} = ( $mm_r->{min_cname_ttl} >= 60 ) ? 0 : 1;
    }

    if ( exists $mm_r->{ip_ttl} ) {
      $mm_r->{min_ip_ttl} = min( @{ $mm_r->{ip_ttl} } );
      $mm_r->{ttl_metric}{ip_ttl} = ( $mm_r->{min_ip_ttl} >= 60 ) ? 0 : 1;
    }

    if ( exists $mm_r->{mx_ttl} ) {
      $mm_r->{max_mx_ttl} = max( @{ $mm_r->{mx_ttl} } );
      $mm_r->{ttl_metric}{mx_ttl} = ( $mm_r->{max_mx_ttl} < 3600 ) ? 1 : 0;
    }

    if ( exists $mm_r->{mx_ip_ttl} ) {
      $mm_r->{max_mx_ip_ttl} = max( @{ $mm_r->{mx_ip_ttl} } );
      $mm_r->{ttl_metric}{mx_ip_ttl} = ( $mm_r->{max_mx_ip_ttl} < 600 ) ? 1 : 0;
    }
  } ## end while ( my ( $dom, $mm_r ...))

  return \%mm;
} ## end sub calc_ttl_diversity_metric

sub calc_ns_diversity_metric {
  my ( $qsld ) = @_;

  my @stat;
  my %see;

  my @d = (
    [ $qsld,       'NS' ],
    [ $qsld,       'MX' ],
    [ "www.$qsld", 'A' ],
    [ "www.$qsld", 'AAAA' ],
  );

  for my $qr ( @d ) {
    my ( $qd, $qt ) = @$qr;
    my $f           = "$DATA_DIR/query-$qd-$qt.log";
    my @record_list = map { [ split /\s+/, $_ ] } read_file( $f );
    for my $record_r ( @record_list ) {
      my ( $dom, $ttl, $class, $type, $rr ) = @$record_r;

      #next unless($dom eq $qd);
      next unless ( $type eq 'NS' );

      for my $at ( qw/A AAAA/ ) {
        my $a_f         = "$DATA_DIR/query-$rr-$at.log";
        my $ns_ip_ttl_r = parse_a_info( $rr, $a_f );
        while ( my ( $ns_ip, $ns_ip_ttl ) = each %$ns_ip_ttl_r ) {
          my $ip_info_f = "$DATA_DIR/ipinfo-$ns_ip.json";
          my $r         = parse_ipinfo( $ip_info_f );

          my $prim = join( "|", $dom, $rr, $ns_ip );
          next if ( exists $see{$prim} );
          push @stat, [ $dom, $rr, $ttl, $ns_ip, $ns_ip_ttl, $r->{org}, $r->{anycast} ? 1 : 0, $r->{country}, $r->{region} ];
          $see{$prim} = 1;
        }
      }

    } ## end for my $record_r ( @record_list)
  } ## end for my $qr ( @d )

  my $head_r = [qw/dom ns ns_ttl ns_ip ns_ip_ttl ns_ip_as ns_ip_anycast ns_ip_country ns_ip_region/];
  my $ns_f   = "$DATA_DIR/metric-ns-$qsld.csv";
  write_table( \@stat, file => $ns_f, sep => '|', head => $head_r );

  my %ns_d;
  tie %ns_d, 'Tie::IxHash';
  push @{ $ns_d{ $_->[0] } }, $_ for @stat;

  #dump (%ns_d);

  my @ns_diversity;

  while ( my ( $dom, $ns_r ) = each %ns_d ) {

    #print "dom : $dom\n";
    my %m;
    $m{dom}     = $dom;
    $m{ns_data} = $ns_r;

    my %ns_cnt = map { $_->[1] => 1 } @$ns_r;
    $m{ns_cnt} = scalar( keys( %ns_cnt ) );
    $m{ns_diversity_metric}{ns_cnt} = $m{ns_cnt} < 2 ? 1 : 0;

    my %ns_ip_cnt = map { $_->[3] => $_->[6] } @$ns_r;
    $m{ns_ip_cnt} = scalar( keys( %ns_ip_cnt ) );
    $m{ns_diversity_metric}{ns_ip_cnt} = $m{ns_ip_cnt} < 2 ? 1 : 0;

    my %ns_ip_as_cnt = map { $_->[5] //= ''; $_->[5] => 1 } @$ns_r;
    $m{ns_ip_as_cnt} = scalar( keys( %ns_ip_as_cnt ) );
    $m{ns_diversity_metric}{ns_ip_as_cnt} = $m{ns_ip_as_cnt} < 2 ? 1 : 0;

    $m{ns_ip_anycast_cnt} = sum_arrayref( [ values( %ns_ip_cnt ) ] );
    $m{ns_diversity_metric}{ns_ip_anycast_cnt} = $m{ns_ip_anycast_cnt} < 1 ? 1 : 0;

    my %ns_ip_country_cnt = map { $_->[7] => 1 } @$ns_r;
    $m{ns_ip_country_cnt} = scalar( keys( %ns_ip_country_cnt ) );

    $m{ns_diversity_metric}{ns_ip_country_cnt} = ( $m{ns_ip_anycast_cnt} < 1 and $m{ns_ip_country_cnt} < 2 ) ? 1 : 0;

    my @ns_ttl = sort { $b <=> $a } map { $_->[2] } @$ns_r;
    $m{max_ns_ttl} = $ns_ttl[0];

    my @temp = split /\./, $dom;
    $m{dom_level} = @temp;

    $m{ttl_metric}{ns_ttl} =
        ( $m{dom_level} < 3  and $m{max_ns_ttl} < 86400 ) ? 1
      : ( $m{dom_level} >= 3 and $m{max_ns_ttl} < 3600 )  ? 1
      :                                                     0;

    my @ns_ip_ttl = sort { $b <=> $a } map { $_->[4] } @$ns_r;
    $m{max_ns_ip_ttl} = $ns_ip_ttl[0];

    $m{ttl_metric}{ns_ip_ttl} =
        ( $m{dom_level} < 3  and $m{max_ns_ip_ttl} < 86400 ) ? 1
      : ( $m{dom_level} >= 3 and $m{max_ns_ip_ttl} < 600 )   ? 1
      :                                                        0;

    $m{ns_diversity_metric_main} = sum_arrayref( [ values( %{ $m{ns_diversity_metric} } ) ] );
    push @ns_diversity, \%m;
  } ## end while ( my ( $dom, $ns_r ...))

  return \@ns_diversity;
} ## end sub calc_ns_diversity_metric

sub parse_ipinfo {
  my ( $f ) = @_;
  my $c     = slurp( $f );
  my $r     = decode_json( $c );
  return $r;
}

sub parse_a_info {
  my ( $qd, $f ) = @_;
  my %see;
  my @record_list = map { [ split /\s+/, $_ ] } read_file( $f );
  for my $record_r ( @record_list ) {
    my ( $dom, $ttl, $class, $type, $rr ) = @$record_r;
    next unless ( $dom eq $qd );
    next unless ( $type eq 'A' or $type eq 'AAAA' );
    $see{$rr} = $ttl;
  }
  return \%see;
}

sub calc_domain_status_metric {
  my ( $qsld ) = @_;

  my $whois_f = "$DATA_DIR/whois-$qsld.log";
  system( qq[whois $qsld > $whois_f] ) unless ( -f $whois_f );

  my $c = slurp( $whois_f );
  my %st;
  $st{clientUpdate}   = ( $c =~ /\nDomain Status: clientUpdateProhibited/ )   ? 0 : 1;
  $st{serverUpdate}   = ( $c =~ /\nDomain Status: serverUpdateProhibited/ )   ? 0 : 1;
  $st{clientDelete}   = ( $c =~ /\nDomain Status: clientDeleteProhibited/ )   ? 0 : 1;
  $st{serverDelete}   = ( $c =~ /\nDomain Status: serverDeleteProhibited/ )   ? 0 : 1;
  $st{clientTransfer} = ( $c =~ /\nDomain Status: clientTransferProhibited/ ) ? 0 : 1;
  $st{serverTransfer} = ( $c =~ /\nDomain Status: serverTransferProhibited/ ) ? 0 : 1;

  my $m = sum_arrayref( [ values( %st ) ] );

  #print "whois metric: $whois_f, domain_status_metric: $m\n";

  my $sf = "$DATA_DIR/metric-status-$qsld.csv";
  write_table( [ [qw/dom metric/], [ $qsld, $m ] ], file => $sf, sep => '|' );

  return { $qsld => { domain_status => \%st, domain_status_metric => $m, whois => $c } };
} ## end sub calc_domain_status_metric

sub get_sld {
  my ( $qsld ) = @_;

  my $whois_f = "$DATA_DIR/whois-$qsld.log";
  system( qq[whois $qsld > $whois_f] ) unless ( -f $whois_f );

  my @d = (
    [ $qsld,       'NS' ],
    [ $qsld,       'MX' ],
    [ "www.$qsld", 'A' ],
    [ "www.$qsld", 'AAAA' ],
  );

  for my $dr ( @d ) {
    my ( $qd, $qt ) = @$dr;
    my $f = "$DATA_DIR/query-$qd-$qt.log";
    system( qq[drill -T $qd $qt > $f] ) unless ( -f $f );

    my @record_list = map { [ split /\s+/, $_ ] } read_file( $f );
    for my $record_r ( @record_list ) {
      my ( $dom, $ttl, $class, $type, @rr_s ) = @$record_r;
      my $rr = $rr_s[-1];
      if ( $type eq 'NS' or $type eq 'MX' ) {
        for my $at ( qw/A AAAA/ ) {
          my $ns_f = "$DATA_DIR/query-$rr-$at.log";
          print $ns_f, "\n";
          system( qq[drill -T $rr $at > $ns_f] ) unless ( -f $ns_f );
          get_a_info( $ns_f );
        }
      } elsif ( $type eq 'A' or $type eq 'AAAA' ) {
        get_ip_info( $rr );
      }
    }
  } ## end for my $dr ( @d )

} ## end sub get_sld

sub get_a_info {
  my ( $f ) = @_;
  my @record_list = map { [ split /\s+/, $_ ] } read_file( $f );
  for my $record_r ( @record_list ) {
    my ( $dom, $ttl, $class, $type, $rr ) = @$record_r;
    if ( $type eq 'A' or $type eq 'AAAA' ) {
      get_ip_info( $rr );
    }
  }
}

sub get_ip_info {
  my ( $ip ) = @_;
  my $f = "$DATA_DIR/ipinfo-$ip.json";
  print $f, "\n";
  system( qq[curl -s https://ipinfo.io/$ip/json -o $f] ) unless ( -f $f );
  return $f;
}
