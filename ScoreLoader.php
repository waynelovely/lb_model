<?php

/**
 * ScoreLoader.php
 * Wayne Lovely - May 2013
 *
 * A class for generating sample data for a leaderboard
 *
 * A test cases are partitioned into "buckets" (tables) as they are
 * generated to make aggregate reporting functions easier.
 *
 * Could use some work, but this as hacked out overnight :)
 *
 */
class ScoreLoader
{
    private $_dbh;
    private $_host     = 'localhost';
    private $_dbname   = 'XXXXX';
    private $_user     = 'YYYYY';
    private $_pass     = 'challenge';

    private $_users    = array();
    private $_userKeys = array();
    private $_numUsers = 0;

    private $_debugOn  = 0;

    /**
     * constructor, grab a database connection
     * 
     * @return none
     */
    function __construct()
    {
        date_default_timezone_set('America/Detroit');

        try {  
            $connectString = "mysql:host=" . $this->_host . 
                                ";dbname=" . $this->_dbname;
            $this->_dbh = new PDO($connectString, $this->_user, $this->_pass);  
        }  catch(PDOException $e) {  
            echo $e->getMessage();  
        }  
    }

    /**
     * destructor, release database connection
     * 
     * @return none
     */
    function __destruct()
    {
        if ($this->_dbh) {
            $this->_dbh = null;
        }
    }


    /**
     * getDbh 
     *
     * gain access to database handle used by instance for external queries
     * 
     * @return none
     */
    function getDbh()
    {
        return $this->_dbh;
    }

    /**
     * setDebug
     *
     * Alter the debug flag state
     * 
     * @param boolean $state control whether debug messages are printed
     *
     * @return none
     */
    function setDebug($state)
    {
        $this->_debugOn = $state;
    }

    /**
     * generateUsers
     *
     * Establish a set of random user_id codes
     * 
     * @return none
     */
    function generateUsers()
    {
        for ($i=0; $i<10000; $i++) {
            $key = "10000" . mt_rand(1000000000, 2000000000);
            $this->_users[ $key ] = 0;
            $this->_userKeys[]    = $key;
            $this->_numUsers++;
        }
    }


    /**
     * generateSamples
     *
     * Generate roughly 1 million sample entries over the course of March, 2013
     *
     * Data is aggegated as we go so we can have fast reports.
     * Data is partitioned in buckets corresponding to 
     * current/daily/weekly reporting needs
     * 
     * @return none
     */
    function generateSamples()
    {
        $days         = 31;
        $intervals    = $days * 24 * 60;
        $samples      = 0;

        $currentStamp = strtotime('3/1/2013');

        for ($i=1; $i<=$intervals; $i++ ) {

            $this->debug(
                $currentStamp . "\t" . date("Y-m-d H:i:s", $currentStamp) . "\n"
            );

            $sampleSize = mt_rand(15, 30);
            $samples += $sampleSize;

            $timestamp     = date("Y-m-d H:i:s", $currentStamp);
            $dateSubmitted = date("Ymd", $currentStamp);
            $dayOfWeek= date("w", $currentStamp);

            $backOff = 86400 * $dayOfWeek;
            $startOfWeek = date("Ymd", $currentStamp - $backOff);

            $prevStartOfWeek = date(
                "Ymd", $currentStamp - (86400 * $dayOfWeek) - (86400 * 7)
            );




            $this->makeTables($startOfWeek, $dateSubmitted);


            // see if we have an aggregate table for the previous week
            $sql = "SELECT count(*) FROM information_schema.tables 
                    WHERE table_schema = 'SAMPLE_DATABASE' AND 
                    table_name = 'weeklyPlayers$prevStartOfWeek' LIMIT 1";
            $this->debug($sql . "\n");
            $prev_week_h = $this->_dbh->prepare($sql);
            $prev_week_h->execute();

            $prevWeekCount = $prev_week_h->fetch();

            for ( $sampleRun=0; $sampleRun<$sampleSize; $sampleRun++ ) {

                $index         = mt_rand(0, $this->_numUsers - 1);
                $user_id       = $this->_userKeys[$index];
                $score         = mt_rand(0, 250000) + mt_rand(0, 250000) + 
                                    mt_rand(0, 250000) + mt_rand(0, 250000);

                $this->debug(
                    "$sampleRun\t$index\t$user_id\t$score\t" .
                    "$timestamp\t$dateSubmitted\n"
                );

                if ( $prevWeekCount[0] == 0 ) {
                    // seed this weeks table with the same score
                    $lastWeekScore = 0;
                    $this->debug(
                        "NO TABLE for weeklyPlayers$prevStartOfWeek " . 
                        "$user_id\n"
                    );
                } else {
                    $this->debug(
                        "MAYBE DATA for weeklyPlayers$prevStartOfWeek " . 
                        "$user_id\n"
                    );

                    // get last weeks data
                    $sql = "select current_high_score from 
                        weeklyPlayers$prevStartOfWeek where user_id = :user_id";
                    $this->debug($sql . "\n");

                    $prev_week_score_h = $this->_dbh->prepare($sql);
                    $prev_week_score_h->bindParam(':user_id', $user_id);
                    $prev_week_score_h->execute();

                    $lastWeekData = $prev_week_score_h -> fetch();

                    if ( $lastWeekData ) {
                        $lastWeekScore = $lastWeekData['current_high_score'];
                    } else {
                        $lastWeekScore = 0;
                    }
                }

                $this->populateWeeklyBoard(
                    $startOfWeek, $index, $user_id, $lastWeekScore, 
                    $prevStartOfWeek, $score, $startOfWeek
                );

                $this->populateDailyLog(
                    $dateSubmitted, $sampleRun, $index, 
                    $user_id, $score, $timestamp
                );

                $this->populateDailyLeader(
                    $dateSubmitted, $sampleRun, $index, 
                    $user_id, $score, $timestamp
                );

                $this->populateMainBoard($index, $user_id, $score, $timestamp);
            }
    
            $currentStamp += 60;
        }
    }


    /**
     * populateDailyLog
     *
     * insert a record into the partitioned daily log table
     * 
     * @param string $dateSubmitted what date are we running on
     * @param int    $sampleRun which run is this
     * @param  $index
     * @param  $user_id
     * @param  $score
     * @param  $timestamp
     *
     * @return none
     */
    function populateDailyLog(
        $dateSubmitted, $sampleRun, $index, $user_id, $score, $timestamp
    ) {
        $sql = "insert into dailyLog" . $dateSubmitted . 
                " (user_id, score, timestamp) " . 
                "values(:user_id, :score, :timestamp)";
        $sth = $this->_dbh->prepare($sql);

        $sth->bindParam(':user_id',   $user_id);
        $sth->bindParam(':score',     $score);
        $sth->bindParam(':timestamp', $timestamp);

        $rowsAffected = $sth -> execute();

        if ( $rowsAffected == 0 ) {
            die("$sampleRun\t$index\t$user_id\t$score");
        } else {
            $this->debug(
                "INSERT\tdailyLog$dateSubmitted\t$index" . 
                "\t$user_id\t$score\t$timestamp\n"
            );
        }
    }

    /**
     * populateDailyLeader
     *
     * keep the daily high score for each player in a tables split by day
     * 
     * @param string $dateSubmitted what date are we running on
     * @param int    $sampleRun which run is this
     * @param  $index
     * @param  $user_id
     * @param  $score
     * @param  $timestamp
     *
     * @return none
     */
    function populateDailyLeader(
        $dateSubmitted, $sampleRun, $index, $user_id, $score, $timestamp
    ) {
        $sql = "select score from dailyPlayers" . $dateSubmitted . 
                "  where user_id = :user_id";
        $player_h = $this->_dbh->prepare($sql);
        $player_h->bindParam(':user_id', $user_id);
        $player_h->execute();

        $playerData = $player_h -> fetch();

        if ( !$playerData ) {
            $sql = "insert into dailyPlayers" . $dateSubmitted . 
                    "  (user_id, score, timestamp) " . 
                    "values (:user_id, :score, :timestamp)";
            $sth = $this->_dbh->prepare($sql);

            $sth->bindParam(':user_id',   $user_id);
            $sth->bindParam(':score',     $score);
            $sth->bindParam(':timestamp', $timestamp);

            $rowsAffected = $sth -> execute();

            if ( $rowsAffected == 0 ) {
                die("$sampleRun\t$index\t$user_id\t$score");
            } else {
                $this->debug(
                    "INSERT\tdailyPlayers$dateSubmitted\t" . 
                    "$index\t$user_id\t$score\t$timestamp\n"
                );
            }

        } else {

            if ( $score > $playerData['score'] ) {
                $sql = "update dailyPlayers" . $dateSubmitted . 
                        "   set score = :score, " . 
                        "timestamp = :timestamp " . 
                        "where user_id = :user_id";

                $sth = $this->_dbh->prepare($sql);

                $sth->bindParam(':user_id',   $user_id);
                $sth->bindParam(':score',     $score);
                $sth->bindParam(':timestamp', $timestamp);

                $rowsAffected = $sth->execute();

                if ( $rowsAffected == 0 ) {
                    die("$sampleRun\t$index\t$user_id\t$score");
                } else {
                    $this->debug(
                        "UPDATE\tdailyPlayers$dateSubmitted\t" . 
                        "$index\t$user_id\t$score\t$timestamp\n"
                    );
                }
            } else {
                $this->debug(
                    "LOW\tdailyPlayers$dateSubmitted\t$index\t" . 
                    "$user_id\t$score\t$timestamp\n"
                );
            }

        }
    }

    /**
     * populateWeeklyBoard
     *
     * keep the weekly high score for each player in a tables split by week
     * 
     * @param    $startOfWeek
     * @param    $index
     * @param    $user_id
     * @param    $lastWeekScore
     * @param    $prevStartOfWeek
     * @param    $score
     * @param    $startOfWeek
     *
     * @return none
     */
    function populateWeeklyBoard(
        $startOfWeek, $index, $user_id, $lastWeekScore, 
        $prevStartOfWeek, $score, $startOfWeek
    ) {
        // either insert or update into current weekly table
        $sql = "select current_high_score, prev_high_score from " . 
                " weeklyPlayers" . $startOfWeek . 
                "  where user_id = :user_id";
        $this->debug($sql . "\n");

        $player_h = $this->_dbh->prepare($sql);
        $player_h->bindParam(':user_id', $user_id);
        $player_h->execute();

        $playerData = $player_h -> fetch();

        if ( !$playerData ) {
            // if the record is missing, seed the table
            $sql = "insert into weeklyPlayers" . $startOfWeek . 
                    "(user_id,prev_high_score,prev_endofweek," . 
                    "current_high_score,current_endofweek,diff) " . 
                    "values(:user_id,:prev_high_score," . 
                    ":prev_endofweek,:current_high_score," . 
                    ":current_endofweek,:diff)";

            $sth = $this->_dbh->prepare($sql);

            $zero = 0;

            $sth->bindParam(':user_id', $user_id);
            $sth->bindParam(':prev_high_score', $lastWeekScore);
            $sth->bindParam(':prev_endofweek', $prevStartOfWeek);
            $sth->bindParam(':current_high_score', $score);
            $sth->bindParam(':current_endofweek', $startOfWeek);
            $sth->bindParam(':diff', $zero);

            $rowsAffected = $sth -> execute();

            if ( $rowsAffected == 0 ) {
                die(
                    "DIE\tWEEKLY INSERT\tweeklyPlayers$startOfWeek\t" . 
                    "$index\t$user_id\t$score\t0"
                );
            } else {
                $this->debug(
                    "WEEKLY INSERT\tweeklyPlayers$startOfWeek\t" . 
                    "$index\t$user_id\t$score\t0\n"
                );
            }

        } else {
            // else just update
            if ( $score > $playerData['current_high_score'] ) {

                $diff = $score - $playerData['prev_high_score'];

                $sql = "update weeklyPlayers" . $startOfWeek  . 
                    " set current_high_score = :score, diff = :diff " . 
                    "where user_id = :user_id";

                $sth = $this->_dbh->prepare($sql);

                $sth->bindParam(':user_id',   $user_id);
                $sth->bindParam(':score',     $score);
                $sth->bindParam(':diff',      $diff);

                $rowsAffected = $sth->execute();

                if ( $rowsAffected == 0 ) {
                    die(
                        "DIE\tWEEKLY UPDATE\tweeklyPlayers" . 
                        "$startOfWeek\t$index\t$user_id\t$score\t$diff"
                    );
                } else {
                    $this->debug(
                        "UPDATE\tweeklyPlayers$startOfWeek\t" . 
                        "$index\t$user_id\t$score\t$diff\n"
                    );
                }
            } else {
                $this->debug(
                    "LOW\tweeklyPlayers$startOfWeek\t$index\t" . 
                    "$user_id\t$score\t0\n"
                );
            }
        }


    }

    /**
     * populateMainBoard
     *
     * keep the current high score for each player in the "players" table
     * 
     * @param  $index
     * @param  $user_id
     * @param  $score
     * @param  $timestamp
     *
     * @return none
     */
    function populateMainBoard($index, $user_id, $score, $timestamp)
    {
        // update the main board
        $sql = "select best_score from players where user_id = :user_id";
        $player_h = $this->_dbh->prepare($sql);
        $player_h->bindParam(':user_id', $user_id);
        $player_h->execute();

        $playerData = $player_h->fetch();

        if ( !$playerData ) {
            $sql = "insert into players (user_id, best_score, " . 
                "best_timestamp) values (:user_id, :score, :timestamp)";
            $sth = $this->_dbh->prepare($sql);

            $sth->bindParam(':user_id',   $user_id);
            $sth->bindParam(':score',     $score);
            $sth->bindParam(':timestamp', $timestamp);

            $rowsAffected = $sth -> execute();

            if ( $rowsAffected == 0 ) {
                die("$sampleRun\t$index\t$user_id\t$score");
            } else {
                $this->debug(
                    "INSERT\tMAIN BOARD\t$index\t$user_id\t" . 
                    "$score\t$timestamp\n"
                );
            }

            $this->_users[$user_id]++;
        } else {

            if ( $score > $playerData['best_score'] ) {
                $sql = "update players set best_score = :score," . 
                        " best_timestamp = :timestamp " . 
                        "where user_id = :user_id";

                $sth = $this->_dbh->prepare($sql);

                $sth->bindParam(':user_id',   $user_id);
                $sth->bindParam(':score',     $score);
                $sth->bindParam(':timestamp', $timestamp);

                $rowsAffected = $sth->execute();

                if ( $rowsAffected == 0 ) {
                    die("$sampleRun\t$index\t$user_id\t$score");
                } else {
                    $this->debug(
                        "UPDATE\tMAIN BOARD\t$index\t" . 
                        "$user_id\t$score\t$timestamp\n"
                    );
                }
            } else {
                $this->debug(
                    "LOW\tMAIN BOARD\t$index\t$user_id\t" . 
                    "$score\t$timestamp\n"
                );
            }
        }
    }

    /**
     * makeTables
     *
     * blindly add tables for the daily and weekly logs
     * 
     * @param  $startOfWeek
     * @param  $dateSubmitted
     *
     * @return none
     */
    function makeTables($startOfWeek, $dateSubmitted)
    {
        // make sure we have a weekly log table named 
        // for the start date of the current week
        $sql = "create table weeklyPlayers" . $startOfWeek . 
                " like weeklyPlayersTEMPLATE";
        $this->debug($sql . "\n");

        $player_h = $this->_dbh->prepare($sql);
        $player_h -> execute();


        // make sure we have a dailLog for this date ( simple log )
        $sql = "create table dailyLog" . $dateSubmitted . 
                " like dailyLogTEMPLATE";
        $player_h = $this->_dbh->prepare($sql);
        $player_h -> execute();

        // make sure we have a leader board for the day
        $sql = "create table dailyPlayers" . $dateSubmitted . 
                " like dailyPlayersTEMPLATE";
        $player_h = $this->_dbh->prepare($sql);
        $player_h -> execute();
    }

    /**
     * debug
     *
     * Print debug lines if the private _debugOn flag is true
     *
     * @param string $data
     *
     * @return none
     */
    function debug($data)
    {
        if ( $this->_debugOn ) {
            print $data;
        }
    }

}

?>
