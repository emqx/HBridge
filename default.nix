{ mkDerivation, base, parsec, stdenv }:
mkDerivation {
  pname = "HBridge";
  version = "0.1.0.0";
  src = ./.;
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [ base parsec ];
  executableHaskellDepends = [ base ];
  homepage = "https://github.com/emqx/HBridge";
  license = stdenv.lib.licenses.bsd3;
}
