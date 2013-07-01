<?php

/*
 * Just a wrapper script to grab an instance of ScoreLoader and do work
 */

require_once('ScoreLoader.php');

$sLoader = new ScoreLoader();

$sLoader->setDebug(true);

$sLoader -> generateUsers();

$sLoader -> generateSamples();

?>
