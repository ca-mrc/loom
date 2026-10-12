import js from "@eslint/js";
import stylistic from "@stylistic/eslint-plugin";
import tsPlugin from "@typescript-eslint/eslint-plugin";
import { defineConfig } from "eslint/config";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import globals from "globals";

export default defineConfig([
  { ignores: ["dist/**", "coverage/**"] },
  {
    files: ["**/*.{ts,tsx}"],
    extends: [js.configs.recommended, ...tsPlugin.configs["flat/recommended"]],
    languageOptions: {
      ecmaVersion: 2022,
      globals: globals.browser,
    },
    plugins: {
      "@stylistic": stylistic,
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
    },
    rules: {
      // Preserve the pre-migration Hooks rules without opting into the React
      // Compiler rules bundled with the newer plugin's recommended preset.
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",
      "react-refresh/only-export-components": "warn",
      // ESLint 10 removes these core stylistic rules; retain their coverage.
      "@stylistic/no-extra-semi": "error",
      "@stylistic/no-mixed-spaces-and-tabs": "error",
      // These checks left the recommended presets or moved to ESLint core.
      "no-class-assign": "error",
      "no-inner-declarations": ["error", "functions", { blockScopedFunctions: "disallow" }],
      "no-constant-condition": ["error", { checkLoops: "all" }],
      "no-loss-of-precision": "error",
      "no-with": "error",
      // This newly recommended rule is outside the existing lint contract.
      "no-useless-assignment": "off",
    },
  },
]);
