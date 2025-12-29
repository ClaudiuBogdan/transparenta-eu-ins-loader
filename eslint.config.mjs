import eslint from "@eslint/js";
import prettierConfig from "eslint-config-prettier";
import comments from "eslint-plugin-eslint-comments";
import { flatConfigs as importPluginFlatConfigs } from "eslint-plugin-import-x";
import promise from "eslint-plugin-promise";
import unicorn from "eslint-plugin-unicorn";
import globals from "globals";
import tseslint from "typescript-eslint";

export default tseslint.config(
  // ========================================================================
  // Base Setup & Global Ignores
  // ========================================================================
  {
    ignores: [
      "dist",
      "node_modules",
      "coverage",
      "data",
      "**/*.d.ts",
      ".git/**",
    ],
  },

  // ========================================================================
  // Recommended Configs
  // ========================================================================
  eslint.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  ...tseslint.configs.stylisticTypeChecked,
  importPluginFlatConfigs.recommended,
  importPluginFlatConfigs.typescript,
  promise.configs["flat/recommended"],

  // ========================================================================
  // Plugin Configuration
  // ========================================================================
  {
    plugins: {
      unicorn,
      "eslint-comments": comments,
    },
    languageOptions: {
      globals: {
        ...globals.node,
      },
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    settings: {
      "import-x/resolver": {
        typescript: true,
        node: true,
      },
    },
    rules: {
      // ====================================================================
      // ASYNC SAFETY & RELIABILITY
      // ====================================================================
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-misused-promises": [
        "error",
        { checksVoidReturn: { attributes: false } },
      ],
      "@typescript-eslint/require-await": "error",

      // ====================================================================
      // DATA SAFETY
      // ====================================================================
      "@typescript-eslint/strict-boolean-expressions": "off",
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/no-unnecessary-condition": "off",
      eqeqeq: ["error", "smart"],

      // ====================================================================
      // CODING STANDARDS & MAINTAINABILITY
      // ====================================================================
      "eslint-comments/require-description": "error",
      "eslint-comments/no-unlimited-disable": "error",

      // Naming Conventions
      "unicorn/filename-case": ["error", { case: "kebabCase" }],
      "@typescript-eslint/naming-convention": [
        "error",
        {
          selector: "default",
          format: ["camelCase"],
          leadingUnderscore: "allow",
        },
        {
          selector: "import",
          format: ["camelCase", "PascalCase"],
        },
        {
          selector: "variable",
          format: ["camelCase", "PascalCase", "UPPER_CASE"],
        },
        { selector: "typeLike", format: ["PascalCase"] },
        { selector: "enumMember", format: ["UPPER_CASE"] },
        {
          selector: "typeProperty",
          format: ["camelCase", "snake_case", "PascalCase"],
        },
        { selector: "objectLiteralProperty", format: null },
      ],

      // Import Ordering
      "import-x/order": [
        "error",
        {
          groups: [
            "builtin",
            "external",
            "internal",
            ["parent", "sibling", "index"],
            "object",
            "type",
          ],
          "newlines-between": "always",
          alphabetize: { order: "asc", caseInsensitive: true },
        },
      ],
      "import-x/no-duplicates": ["error", { "prefer-inline": true }],
      "import-x/no-cycle": "error",
      "import-x/no-named-as-default-member": "off",

      // Unused Vars
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],

      // Type Imports
      "@typescript-eslint/consistent-type-imports": "error",
      "@typescript-eslint/no-import-type-side-effects": "error",
    },
  },

  // ========================================================================
  // TEST FILES
  // ========================================================================
  {
    files: ["**/*.test.ts", "**/*.spec.ts", "tests/**/*.ts"],
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/no-unsafe-argument": "off",
      "@typescript-eslint/require-await": "off",
      "@typescript-eslint/unbound-method": "off",
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/no-unnecessary-condition": "off",
      "@typescript-eslint/strict-boolean-expressions": "off",
      "no-magic-numbers": "off",
    },
  },

  // ========================================================================
  // CONFIG FILES (No type checking needed)
  // ========================================================================
  {
    files: ["*.config.{js,ts,mjs,cjs}", "vitest.config.ts"],
    ...tseslint.configs.disableTypeChecked,
  },

  // ========================================================================
  // Prettier (Must be last)
  // ========================================================================
  prettierConfig
);
