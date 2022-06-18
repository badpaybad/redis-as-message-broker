<?php

// autoload_static.php @generated by Composer

namespace Composer\Autoload;

class ComposerStaticInitabd06069d0a25759359eae99c262d677
{
    public static $prefixLengthsPsr4 = array (
        'a' => 
        array (
            'app\\' => 4,
        ),
    );

    public static $prefixDirsPsr4 = array (
        'app\\' => 
        array (
            0 => __DIR__ . '/../..' . '/app',
        ),
    );

    public static $classMap = array (
        'Composer\\InstalledVersions' => __DIR__ . '/..' . '/composer/InstalledVersions.php',
    );

    public static function getInitializer(ClassLoader $loader)
    {
        return \Closure::bind(function () use ($loader) {
            $loader->prefixLengthsPsr4 = ComposerStaticInitabd06069d0a25759359eae99c262d677::$prefixLengthsPsr4;
            $loader->prefixDirsPsr4 = ComposerStaticInitabd06069d0a25759359eae99c262d677::$prefixDirsPsr4;
            $loader->classMap = ComposerStaticInitabd06069d0a25759359eae99c262d677::$classMap;

        }, null, ClassLoader::class);
    }
}
